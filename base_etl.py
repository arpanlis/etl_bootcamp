import os

from connection import get_connection
from constants import DIMENSION_TABLES, EXPORT_FOLDER, SCHEMAS


class BaseETL:
    table: str
    source_schema: str

    staging_ddl: str
    temp_ddl: str
    target_ddl: str

    def __init__(self) -> None:
        # Create export folder if not exists
        if not os.path.exists(EXPORT_FOLDER):
            os.makedirs(EXPORT_FOLDER)

        # Create schemas if not exists
        conn = get_connection()
        cur = conn.cursor()

        for schema in SCHEMAS:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        cur.close()

        # Create tables in all schemas
        for schema in SCHEMAS:
            if schema == "STG":
                table = self.STAGING_TABLE
                ddl = self.staging_ddl
            elif schema == "TMP":
                table = self.TEMP_TABLE
                ddl = self.temp_ddl
            else:
                table = self.TARGET_TABLE
                ddl = self.target_ddl

            self.run_ddl(table=table, ddl=ddl, schema=schema)

    def run_ddl(self, table: str, ddl: str, schema: str):
        conn = get_connection(schema=schema)
        cur = conn.cursor()

        ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table}({ddl});"

        cur.execute(ddl)

        cur.close()

    @property
    def STAGING_TABLE(self):
        f_or_d = "D" if self.table in DIMENSION_TABLES else "F"
        postfix = "LU" if self.table in DIMENSION_TABLES else "TRXN_B"
        return f"STG_{f_or_d}_{self.table}_{postfix}"

    @property
    def TEMP_TABLE(self):
        f_or_d = "D" if self.table in DIMENSION_TABLES else "F"
        postfix = "LU" if self.table in DIMENSION_TABLES else "TRXN_B"
        return f"TMP_{f_or_d}_{self.table}_{postfix}"

    @property
    def TARGET_TABLE(self):
        f_or_d = "D" if self.table in DIMENSION_TABLES else "F"
        postfix = "LU" if self.table in DIMENSION_TABLES else "TRXN_B"
        return f"DWH_{f_or_d}_{self.table}_{postfix}"

    def local_to_staging(self):
        conn = get_connection(schema="STG")

        cur = conn.cursor()

        # Create import stage
        cur.execute("CREATE OR REPLACE STAGE ImportStage")

        # Copy data from local csv to import stage
        cur.execute(
            f"""
                PUT file://{EXPORT_FOLDER}/{self.table}_export.csv_0_0_0.csv.gz @ImportStage/imp
            """
        )

        # Truncate staging table
        cur.execute(f"TRUNCATE TABLE {self.STAGING_TABLE}")

        # Copy data from import stage to staging table
        copy_query = f"""
            COPY INTO {self.STAGING_TABLE}
            FROM '@ImportStage/imp/{self.table}_export.csv'
            FILE_FORMAT = (TYPE = CSV)
        """

        cur.execute(copy_query)

        print(f"Data has been copied to {self.STAGING_TABLE}")

        cur.close()

    def extract(self):
        source_conn = get_connection(schema=self.source_schema)

        # Cursor to execute queries
        source_cur = source_conn.cursor()

        # Create local stage
        source_cur.execute("CREATE OR REPLACE STAGE ExportStage")

        # Query to copy data from Snowflake table to stage
        copy_query = f"""
        COPY INTO @ExportStage/exp/{self.table}_export.csv
        FROM {self.table}
        FILE_FORMAT = (TYPE = CSV)
        """

        source_cur.execute(copy_query)

        # Download data from stage to local file
        source_cur.execute(
            f"GET @ExportStage/exp/{self.table}_export.csv file://{EXPORT_FOLDER}"
        )

        print(f"Data has been downloaded to {EXPORT_FOLDER}/{self.table}_export.csv")

        # Close cursor and connection
        source_cur.close()

    def transform(self):
        conn = get_connection(schema="TMP")
        cur = conn.cursor()

        # Truncate temp table
        cur.execute(f"TRUNCATE TABLE {self.TEMP_TABLE}")

        # Insert data into temp table
        cur.execute(
            f"INSERT INTO {self.TEMP_TABLE} SELECT * FROM STG.{self.STAGING_TABLE}"
        )

        print(f"{self.STAGING_TABLE} data has been transformed to {self.TEMP_TABLE}")

        cur.close()

    def load(self):
        raise NotImplementedError

    def run(self):
        self.extract()
        self.local_to_staging()
        self.transform()
        self.load()
