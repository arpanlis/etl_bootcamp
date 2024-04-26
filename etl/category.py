from core.base_etl import BaseETL
from core.connection import get_connection


class CategoryETL(BaseETL):
    table = "CATEGORY"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
        ID NUMBER(38,0) NOT NULL,
        CATEGORY_DESC VARCHAR(1024),
        primary key (ID)
    """

    temp_ddl = """
        ID NUMBER(38,0) NOT NULL,
        CATEGORY_DESC VARCHAR(1024),
        PRIMARY KEY (ID)
    """

    target_ddl = """
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        CATEGORY_DESC VARCHAR(1024),
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
            MERGE INTO TGT.DWH_D_CATEGORY_LU TGT
            USING (
                SELECT
                    SRC.ID,
                    SRC.CATEGORY_DESC
                FROM TMP.TMP_D_CATEGORY_LU SRC
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
            WHEN MATCHED AND (TGT.CATEGORY_DESC <> SRC.CATEGORY_DESC)
            THEN
                UPDATE
                SET TGT.CATEGORY_DESC = SRC.CATEGORY_DESC,
                    TGT.START_DATE = CURRENT_TIMESTAMP(),
                    TGT.ACTIVE_FLAG = TRUE
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ID, CATEGORY_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                VALUES (SRC.ID, SRC.CATEGORY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE);
        """
        )

        # Disable autocommit to allow for rollback
        conn.autocommit(False)
        cur = conn.cursor()

        # Type 2 SCD
        try:
            cur.execute(
                """
                    UPDATE TGT.DWH_D_CATEGORY_LU TGT
                    SET CLOSE_DATE = CURRENT_TIMESTAMP(),
                        ACTIVE_FLAG = FALSE
                    WHERE ID_SK IN (
                        SELECT TGT.ID_SK
                        FROM TGT.DWH_D_CATEGORY_LU TGT
                        LEFT JOIN TMP.TMP_D_CATEGORY_LU TMP ON TGT.SOURCE_ID = TMP.ID
                        WHERE TGT.ACTIVE_FLAG = TRUE
                          AND TMP.ID IS NULL
                    )
                """
            )

            cur.execute(
                """
                INSERT INTO TGT.DWH_D_CATEGORY_LU (SOURCE_ID, CATEGORY_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                SELECT
                    SRC.ID,
                    SRC.CATEGORY_DESC,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    TRUE
                FROM TMP.TMP_D_CATEGORY_LU SRC
                LEFT JOIN TGT.DWH_D_CATEGORY_LU TGT ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
                WHERE TGT.SOURCE_ID IS NULL;
                """
            )

            conn.commit()
            cur.close()

        except Exception as e:
            conn.rollback()
            cur.close()
            raise e

        print(f"Data has been loaded into {self.TARGET_TABLE}")
