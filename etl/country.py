from core.base_etl import BaseETL
from core.connection import get_connection


class CountryETL(BaseETL):
    table = "COUNTRY"
    source_schema = "TRANSACTIONS"
    staging_ddl = """
        ID NUMBER(38,0) NOT NULL,
        COUNTRY_DESC VARCHAR(256),
        primary key (ID)
    """
    temp_ddl = """
        ID NUMBER(38,0) NOT NULL,
        COUNTRY_DESC VARCHAR(256),
        PRIMARY KEY (ID)
    """
    target_ddl = """
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        COUNTRY_DESC VARCHAR(256),
        ACTIVE_FLAG BOOLEAN,
        START_DATE TIMESTAMP,
        CLOSE_DATE TIMESTAMP,
        primary key (ID_SK)
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Close dimensions
        cur.execute(
            f"""
            UPDATE {self.TARGET_TABLE}
            SET CLOSE_DATE = CURRENT_TIMESTAMP(),
                ACTIVE_FLAG = FALSE
            WHERE ID_SK IN (
                SELECT ID_SK
                FROM {self.TARGET_TABLE}
                WHERE ACTIVE_FLAG = TRUE
                  AND SOURCE_ID NOT IN (SELECT ID FROM TMP.{self.TEMP_TABLE})
            )
            """
        )

        # Insert or Update
        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING TMP.{self.TEMP_TABLE} SRC
            ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
            WHEN MATCHED AND TGT.COUNTRY_DESC <> SRC.COUNTRY_DESC THEN
                UPDATE
                SET TGT.COUNTRY_DESC = SRC.COUNTRY_DESC,
                    TGT.START_DATE = CURRENT_TIMESTAMP(),
                    TGT.ACTIVE_FLAG = TRUE
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ID, COUNTRY_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                VALUES (SRC.ID, SRC.COUNTRY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE)
            """
        )

        print(f"Data has been loaded into {self.TARGET_TABLE}")
        cur.close()
