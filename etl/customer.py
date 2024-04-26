from core.base_etl import BaseETL
from core.connection import get_connection


class CustomerETL(BaseETL):
    table = "CUSTOMER"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
        ID NUMBER(38,0) NOT NULL,
        CUSTOMER_FIRST_NAME VARCHAR(256),
        CUSTOMER_MIDDLE_NAME VARCHAR(256),
        CUSTOMER_LAST_NAME VARCHAR(256),
        CUSTOMER_ADDRESS VARCHAR(256),
        primary key (ID)
    """

    temp_ddl = """
        ID NUMBER(38,0) NOT NULL,
        CUSTOMER_FIRST_NAME VARCHAR(256),
        CUSTOMER_MIDDLE_NAME VARCHAR(256),
        CUSTOMER_LAST_NAME VARCHAR(256),
        CUSTOMER_ADDRESS VARCHAR(256),
        PRIMARY KEY (ID)
    """

    target_ddl = """
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        CUSTOMER_FIRST_NAME VARCHAR(256),
        CUSTOMER_MIDDLE_NAME VARCHAR(256),
        CUSTOMER_LAST_NAME VARCHAR(256),
        CUSTOMER_ADDRESS VARCHAR(256),
        START_DATE TIMESTAMP,
        CLOSE_DATE TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
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

        # Type 1 SCD
        cur.execute(
            """
            MERGE INTO TGT.DWH_D_CUSTOMER_LU TGT
            USING (
                SELECT
                    SRC.ID,
                    SRC.CUSTOMER_FIRST_NAME,
                    SRC.CUSTOMER_MIDDLE_NAME,
                    SRC.CUSTOMER_LAST_NAME,
                    SRC.CUSTOMER_ADDRESS
                FROM TMP.TMP_D_CUSTOMER_LU SRC
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
            WHEN MATCHED AND (
                TGT.CUSTOMER_FIRST_NAME <> SRC.CUSTOMER_FIRST_NAME OR
                TGT.CUSTOMER_MIDDLE_NAME <> SRC.CUSTOMER_MIDDLE_NAME OR
                TGT.CUSTOMER_LAST_NAME <> SRC.CUSTOMER_LAST_NAME OR
                TGT.CUSTOMER_ADDRESS <> SRC.CUSTOMER_ADDRESS
            )
            THEN
                UPDATE
                SET TGT.CUSTOMER_FIRST_NAME = SRC.CUSTOMER_FIRST_NAME,
                    TGT.CUSTOMER_MIDDLE_NAME = SRC.CUSTOMER_MIDDLE_NAME,
                    TGT.CUSTOMER_LAST_NAME = SRC.CUSTOMER_LAST_NAME,
                    TGT.CUSTOMER_ADDRESS = SRC.CUSTOMER_ADDRESS,
                    TGT.START_DATE = CURRENT_TIMESTAMP(),
                    TGT.ACTIVE_FLAG = TRUE
            WHEN NOT MATCHED THEN
                INSERT (
                    SOURCE_ID,
                    CUSTOMER_FIRST_NAME,
                    CUSTOMER_MIDDLE_NAME,
                    CUSTOMER_LAST_NAME,
                    CUSTOMER_ADDRESS,
                    START_DATE,
                    CLOSE_DATE,
                    ACTIVE_FLAG
                )
                VALUES (
                    SRC.ID,
                    SRC.CUSTOMER_FIRST_NAME,
                    SRC.CUSTOMER_MIDDLE_NAME,
                    SRC.CUSTOMER_LAST_NAME,
                    SRC.CUSTOMER_ADDRESS,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    TRUE
                );
        """
        )

        # Disable autocommit to allow for rollback
        conn.autocommit(False)
        cur = conn.cursor()

        # Type 2 SCD (None specified in the original script)

        try:
            conn.commit()
            cur.close()

        except Exception as e:
            conn.rollback()
            cur.close()
            raise e

        print(f"Data has been loaded into {self.TARGET_TABLE}")
