from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pyodbc
import snowflake.connector
from snowflake.connector import SnowflakeConnection  # Add this import
import csv
from typing import Dict, List
import yaml
import logging
from tempfile import NamedTemporaryFile
import os

live = True
if live:
    # prod
    config_file= "db_config_prod.yaml"
    dir = "/data" # directory on server
    #tables = ['Appointment','AppointmentStatusHistory','BillingChargeTransaction','BillingClaim','BillingClaimEvent','BillingEncounter','BillingPatientLedger','BillingPaymentAdjustmentRefund','BillingSuperbill','BillingSuperbillProcedure','Service']
    tables = ['Table1','Table2']
    table_prefix = "TEST__"
    table_suffix = "_HISTORY"
    schema_db = "PUBLIC"
else:
    # test
    config_file= "db_config.yaml"
    dir = "/data" #full local directory 
    tables = ['Authors','Books','Orders']
    table_prefix = "G1__"
    table_suffix = "_HISTORY"
    schema_db = "PUBLIC"

# Set up log directory and file
log_dir = f"{dir}/logs"


os.makedirs(log_dir, exist_ok=True)  # Create the directory if it doesn't exist
log_file = os.path.join(log_dir, "migration.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),  # Log to file
        logging.StreamHandler()         # Also log to console
    ]
)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.mssql_conn = self._connect_mssql()
        self.snow_conn = self._connect_snowflake()
        self.stage_name = 'MIGRATION_STAGE'

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
        
    def _connect_mssql(self) -> pyodbc.Connection:
        mssql_config = self.config['mssql']

        # Create connection URL
        params = {
            'driver': 'ODBC Driver 17 for SQL Server',
            'TrustServerCertificate': 'yes',
            'Encrypt': 'yes',
            'ConnectionTimeout': '30',
            'ApplicationIntent': 'ReadOnly'
        }

        # Build connection URL
        conn_url = (
            f"mssql+pyodbc://{quote_plus(mssql_config['user'])}:"
            f"{quote_plus(mssql_config['password'])}@"
            f"{mssql_config['server']}/"
            f"{mssql_config['database']}?"
            f"driver={quote_plus(params['driver'])}&"
            f"TrustServerCertificate={params['TrustServerCertificate']}&"
            f"Encrypt={params['Encrypt']}&"
            f"ConnectionTimeout={params['ConnectionTimeout']}&"
            f"ApplicationIntent={params['ApplicationIntent']}"
        )

        try:
            logger.info(f"Attempting to connect to MSSQL server: {mssql_config['server']}")
            engine = create_engine(conn_url)
            return engine.raw_connection()
        except Exception as e:
            logger.error(f"Failed to connect to MSSQL: {str(e)}")
            # Log connection URL without password
            safe_url = conn_url.replace(quote_plus(mssql_config['password']), '****')
            logger.info(f"Connection URL (without password): {safe_url}")
            raise

    def _connect_snowflake(self) -> SnowflakeConnection:  # Changed return type hint
        snow_config = self.config['snowflake']
        return snowflake.connector.connect(
            user=snow_config['user'],
            password=snow_config['password'],
            account=snow_config['account'],
            warehouse=snow_config['warehouse'],
            database=snow_config['database'],
            schema=schema_db
        )


    def get_mssql_tables(self) -> List[str]:
        """Get list of tables from MSSQL schema"""
        #with self.mssql_conn.cursor() as cur:
        #    cur.execute("""
        #        SELECT TABLE_NAME
        #        FROM INFORMATION_SCHEMA.TABLES
        #        WHERE TABLE_SCHEMA = ?
        #        AND TABLE_TYPE = 'BASE TABLE'
        #    """, (self.config['mssql']['schema'],))

        return tables
        #return ["Staff","SupportPerson"]
            #return [row[0] for row in cur.fetchall()]

    def get_table_structure(self, table_name: str) -> List[Dict]:
        """Get column definitions from MSSQL"""
        with self.mssql_conn.cursor() as cur:
            schema_sql ="""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            cur.execute(schema_sql, (self.config['mssql']['schema'], table_name))
            logger.info(f"Executing SQL: {schema_sql} with params: {self.config['mssql']['schema']}, {table_name}")

            columns = []
            for row in cur.fetchall():
                col_name, data_type, max_length, precision, scale = row
                snow_type = self._map_data_type(
                    data_type,
                    max_length,
                    precision,
                    scale
                )
                columns.append({
                    'name': col_name,
                    'mssql_type': data_type,
                    'snowflake_type': snow_type
                })
            print(columns)
            logger.info(f"Table structure for {table_name}: {columns}")

            return columns

    def _map_data_type(self, mssql_type: str, max_length: int = None,
                      precision: int = None, scale: int = None) -> str:
        """Map MSSQL data types to Snowflake data types"""
        type_mapping = {
            'bigint': 'NUMBER(38,0)',
            'bit': 'BOOLEAN',
            'decimal': f'NUMBER({precision or 38},{scale or 0})',
            'int': 'NUMBER(38,0)',
            'money': 'NUMBER(38,4)',
            'numeric': f'NUMBER({precision or 38},{scale or 0})',
            'smallint': 'NUMBER(38,0)',
            'smallmoney': 'NUMBER(38,4)',
            'tinyint': 'NUMBER(38,0)',
            'float': 'FLOAT',
            'real': 'FLOAT',
            'date': 'DATE',
            'datetime': 'TIMESTAMP_TZ',
            'datetime2': 'TIMESTAMP_TZ',
            'smalldatetime': 'TIMESTAMP_TZ',
            'time': 'TIME',
            'datetimeoffset': 'TIMESTAMP_TZ',
            #'char': f'CHAR({max_length if max_length else 1})',
            'char': f'CHAR',
            #'varchar': f'VARCHAR({max_length if max_length else 16777216})',
            'varchar': f'VARCHAR',
            'text': 'TEXT',
            #'nchar': f'CHAR({max_length if max_length else 1})',
            'nchar': f'CHAR',
            #'nvarchar': f'VARCHAR({max_length if max_length else 16777216})',
            'nvarchar': f'VARCHAR',
            'ntext': 'TEXT',
            'binary': 'BINARY',
            'varbinary': 'BINARY',
            'image': 'BINARY',
            'xml': 'VARIANT',
            'uniqueidentifier': 'VARCHAR'
        }
        return type_mapping.get(mssql_type.lower(), 'TEXT')

    def create_snowflake_table(self, table_name: str, columns: List[Dict]) -> str:
        """Create table in Snowflake"""
        snow_schema = schema_db
        prefix = table_prefix
        suffix = table_suffix
        snow_table = f"{prefix}{table_name}{suffix}".upper()

        # Function to properly quote column names
        def quote_column_name(name: str) -> str:
            # List of Snowflake reserved words that need quoting
            reserved_words = {'ORDER', 'KEY', 'LIKE', 'TABLE', 'GROUP', 'BY', 'DESC',
                            'SELECT', 'FROM', 'WHERE', 'JOIN', 'HAVING', 'CASE',
                            'END', 'WHEN', 'THEN', 'ELSE', 'DATE', 'TIME', 'TIMESTAMP',
                            'USER', 'ROLE', 'GRANT', 'DEFAULT'}
   
            # Quote if it's a reserved word or contains special characters
            if name.upper() in reserved_words or not name.isalnum():
                logger.info(f"Quoting column name for reserved word: {name}")
                return f'"{name}"'
            return name

        # Create column definitions with proper quoting
        column_defs = [
            f'{quote_column_name(col["name"])} {col["snowflake_type"]}'
            for col in columns
        ]    
        #column_defs = [f"{col['name']} {col['snowflake_type']}" for col in columns]

        create_sql = f"""
            CREATE OR REPLACE TABLE {snow_schema}.{snow_table} (
                {', '.join(column_defs).upper()}
            )
        """
        logger.info(f"create_sql: {create_sql}")

        with self.snow_conn.cursor() as cur:
            cur.execute(create_sql)
        return snow_table

    def setup_snowflake_stage(self):
        """Create or replace internal stage in Snowflake"""
        with self.snow_conn.cursor() as cur:
            cur.execute(f"CREATE OR REPLACE STAGE {self.stage_name}")

    def transfer_table(self, table_name: str) -> None:
        """Transfer a single table from MSSQL to Snowflake"""
        try:
            logger.info(f"Starting transfer of table {table_name}")

            # Get table structure and create Snowflake table
            columns = self.get_table_structure(table_name)
            snow_table = self.create_snowflake_table(table_name, columns)

            # Quote column names for SQL queries
            column_names = [f'[{col["name"]}]' for col in columns]  # SQL Server style quoting            
            #column_names = [col['name'] for col in columns]

            # Export data from MSSQL to CSV
            with NamedTemporaryFile(mode='w', newline='', delete=False, suffix='.csv') as temp_file:
                logger.info(f"Exporting data from MSSQL to {temp_file.name}")

                # Write CSV header
                writer = csv.writer(temp_file)
                writer.writerow([col["name"] for col in columns])  # Original names for CSV header
                #writer.writerow(column_names)

                # Fetch and write data
                with self.mssql_conn.cursor() as cur:
                    select_sql = f"""
                        SELECT {', '.join(column_names)}
                        FROM {self.config['mssql']['schema']}.{table_name}
                    """
                    cur.execute(select_sql)
                    logger.info(f"Executing SQL: {select_sql}")

                    counter=0
                    while True:
                        rows = cur.fetchmany(10000)  # Process in chunks
                        counter = len(rows) + counter
                        logger.info(f"Chunking {len(counter)} rows for ({table_name}) to {temp_file.name}")
                        if not rows:
                            counter = 0
                            break
                        writer.writerows(rows)

            # Upload to Snowflake stage
            logger.info("Uploading to Snowflake stage")
            with self.snow_conn.cursor() as cur:
                cur.execute(f"PUT file://{temp_file.name} @{self.stage_name}")

                # Copy into Snowflake table
                copy_into_sql = f"""
                    COPY INTO {snow_table}
                    FROM @{self.stage_name}/{os.path.basename(temp_file.name)}
                    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
                    ON_ERROR = CONTINUE
                """
                logger.info(f"Executing COPY INTO SQL: {copy_into_sql}")        
                cur.execute(copy_into_sql)

            # Cleanup
            os.unlink(temp_file.name)
            logger.info(f"Successfully transferred {table_name} to {snow_table}")

        except Exception as e:
            logger.error(f"Error transferring table {table_name}: {str(e)}")
            raise

    def transfer_all_tables(self) -> None:
        """Transfer all tables from MSSQL to Snowflake"""
        try:
            # Setup Snowflake stage
            self.setup_snowflake_stage()

            # Get and transfer all tables
            tables = self.get_mssql_tables()
            logger.info(f"Found {len(tables)} tables to transfer")

            for table in tables:
                self.transfer_table(table)

        finally:
            # Cleanup stage
            with self.snow_conn.cursor() as cur:
                cur.execute(f"DROP STAGE IF EXISTS {self.stage_name}")

    def cleanup(self) -> None:
        """Close all database connections"""
        self.mssql_conn.close()
        self.snow_conn.close()

def main():
    config_path = f'{dir}/{config_file}'

    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    logger.info(f"Using configuration file: {config_path}")

    try:
        migrator = DatabaseMigrator(config_path)
        migrator.transfer_all_tables()
        migrator.cleanup()
        logger.info("Migration completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
