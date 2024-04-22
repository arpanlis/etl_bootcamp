from snowflake.connector.connection import os

from con import get_connection
from constants import DIMENSION_TABLES
from helpers import EXPORT_FOLDER


class BaseETL:
    table = None
    schema = None
    source = None

    def __init__(self) -> None:
        # Create export folder if not exists
        if not os.path.exists(EXPORT_FOLDER):
            os.makedirs(EXPORT_FOLDER)

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
        return f"{f_or_d}_{self.table}_{postfix}"

    def source_to_local_file(self):
        conn = get_connection(schema=self.schema)

        if not os.path.exists(EXPORT_FOLDER):
            os.makedirs(EXPORT_FOLDER)

        # Cursor to execute queries
        cur = conn.cursor()

        # Create local stage
        cur.execute("CREATE OR REPLACE STAGE ExportStage")

        # Query to copy data from Snowflake table to stage
        copy_query = f"""
        COPY INTO @ExportStage/exp/{self.table}_export.csv
        FROM {self.table}
        FILE_FORMAT = (TYPE = CSV)
        """

        cur.execute(copy_query)

        # Download data from stage to local file
        cur.execute(
            f"GET @ExportStage/exp/{self.table}_export.csv file://{EXPORT_FOLDER}"
        )

        print(f"Data has been downloaded to {EXPORT_FOLDER}/{self.table}_export.csv")

        # Close cursor and connection
        cur.close()
        conn.close()

    def extract(self):
        raise NotImplementedError

    def transform(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def run(self):
        self.source_to_local_file()
        self.extract()
        self.transform()
        self.load()
