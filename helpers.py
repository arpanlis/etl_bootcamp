import os

from con import get_connection
from constants import DIMENSION_TABLES, SCHEMAS

EXPORT_FOLDER = "data"


def schema_and_table_init():
    create_schemas(SCHEMAS)

    for schema in SCHEMAS:
        conn = get_connection(schema=schema)

        cur = conn.cursor()

        with open(f"ddl/{schema}.sql") as f:
            ddl_s = f.read().split(";")

            for ddl in ddl_s:
                print(f"Executing {ddl}")
                cur.execute(ddl)

        cur.close()
        conn.close()


def csv_to_staging(table: str):
    conn = get_connection(schema="STG")

    cur = conn.cursor()

    # STG_F_SALES_TRXN_B

    f_or_d = "D" if table in DIMENSION_TABLES else "F"

    postfix = "LU" if table in DIMENSION_TABLES else "TRXN_B"

    staging_table_name = f"STG_{f_or_d}_{table}_{postfix}"

    # Create import stage
    cur.execute("CREATE OR REPLACE STAGE ImportStage")

    # Copy data from local csv to import stage
    cur.execute(
        f"""
    PUT file://{EXPORT_FOLDER}/{table}_export.csv_0_0_0.csv.gz @ImportStage/imp
    """
    )

    # Copy data from import stage to staging table
    copy_query = f"""
        COPY INTO {staging_table_name}
        FROM '@ImportStage/imp/{table}_export.csv'
        FILE_FORMAT = (TYPE = CSV)
    """

    cur.execute(copy_query)

    print(f"Data has been copied to {staging_table_name}")

    cur.close()
    conn.close()


def staging_to_temp(table: str):
    pass


def create_schemas(schemas: list[str]):
    conn = get_connection()

    cur = conn.cursor()

    for schema in schemas:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    cur.close()
    conn.close()


def export_to_local_csv(schema: str, table: str):
    conn = get_connection(schema=schema)

    if not os.path.exists(EXPORT_FOLDER):
        os.makedirs(EXPORT_FOLDER)

    # Cursor to execute queries
    cur = conn.cursor()

    # Create local stage
    cur.execute("CREATE OR REPLACE STAGE ExportStage")

    # Query to copy data from Snowflake table to stage
    copy_query = f"""
    COPY INTO @ExportStage/exp/{table}_export.csv
    FROM {table}
    FILE_FORMAT = (TYPE = CSV)
    """

    cur.execute(copy_query)

    # Download data from stage to local file
    cur.execute(f"GET @ExportStage/exp/{table}_export.csv file://{EXPORT_FOLDER}")

    print(f"Data has been downloaded to {EXPORT_FOLDER}/{table}_export.csv")

    # Close cursor and connection
    cur.close()
    conn.close()
