from core.base_etl import BaseETL
from core.connection import get_connection


class SubcategoryETL(BaseETL):
    table = "SUBCATEGORY"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
        ID NUMBER(38,0) NOT NULL,
        CATEGORY_ID NUMBER(38,0),
        SUBCATEGORY_DESC VARCHAR(256),
        primary key (ID),
        foreign key (CATEGORY_ID) references BHATBHATENI.TRANSACTIONS.CATEGORY(ID)
    """

    temp_ddl = """
        ID NUMBER(38,0) NOT NULL,
        CATEGORY_ID NUMBER(38,0),
        SUBCATEGORY_DESC VARCHAR(256),
        PRIMARY KEY (ID),
        FOREIGN KEY (CATEGORY_ID) REFERENCES TMP.TMP_D_CATEGORY_LU(ID)
    """

    target_ddl = """
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        CATEGORY_ID_SK NUMBER(38,0),
        SUBCATEGORY_DESC VARCHAR(256),
        START_DATE TIMESTAMP,
        CLOSE_DATE TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
        primary key (ID_SK),
        FOREIGN KEY (CATEGORY_ID_SK) REFERENCES TGT.DWH_D_CATEGORY_LU(ID_SK)
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
            MERGE INTO TGT.DWH_D_SUBCATEGORY_LU TGT
            USING (
                SELECT
                    SRC.ID,
                    SRC.CATEGORY_ID,
                    SRC.SUBCATEGORY_DESC,
                    CAT.ID_SK AS CATEGORY_ID_SK
                FROM TMP.TMP_D_SUBCATEGORY_LU SRC
                LEFT JOIN TGT.DWH_D_CATEGORY_LU CAT ON CAT.SOURCE_ID = SRC.CATEGORY_ID AND CAT.ACTIVE_FLAG = TRUE
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
            WHEN MATCHED AND (TGT.SUBCATEGORY_DESC <> SRC.SUBCATEGORY_DESC)
            THEN
                UPDATE
                SET TGT.SUBCATEGORY_DESC = SRC.SUBCATEGORY_DESC,
                    TGT.CATEGORY_ID_SK = SRC.CATEGORY_ID_SK,
                    TGT.START_DATE = CURRENT_TIMESTAMP(),
                    TGT.ACTIVE_FLAG = TRUE
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ID, CATEGORY_ID_SK, SUBCATEGORY_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                VALUES (SRC.ID, SRC.CATEGORY_ID_SK, SRC.SUBCATEGORY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE);
        """
        )

        # Disable autocommit to allow for rollback
        conn.autocommit(False)
        cur = conn.cursor()

        # Type 2 SCD
        try:
            # Mark previous records as closed
            cur.execute(
                """
                    MERGE INTO TGT.DWH_D_SUBCATEGORY_LU TGT
                    USING (
                        SELECT
                            SRC.ID,
                            SRC.CATEGORY_ID,
                            SRC.SUBCATEGORY_DESC,
                            CAT.ID_SK AS CATEGORY_ID_SK
                        FROM TMP.TMP_D_SUBCATEGORY_LU SRC
                        LEFT JOIN TGT.DWH_D_CATEGORY_LU CAT ON CAT.SOURCE_ID = SRC.CATEGORY_ID AND CAT.ACTIVE_FLAG = TRUE
                    ) SRC
                    ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
                    WHEN MATCHED AND TGT.CATEGORY_ID_SK <> SRC.CATEGORY_ID_SK
                    THEN
                        UPDATE
                        SET TGT.CLOSE_DATE = CURRENT_TIMESTAMP(),
                            TGT.ACTIVE_FLAG = FALSE
                """
            )

            cur.execute(
                """
                INSERT INTO TGT.DWH_D_SUBCATEGORY_LU (SOURCE_ID, CATEGORY_ID_SK, SUBCATEGORY_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                SELECT
                    SRC.ID,
                    CAT.ID_SK,
                    SRC.SUBCATEGORY_DESC,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    TRUE
                FROM TMP.TMP_D_SUBCATEGORY_LU SRC
                JOIN TGT.DWH_D_CATEGORY_LU CAT ON CAT.SOURCE_ID = SRC.CATEGORY_ID AND CAT.ACTIVE_FLAG = TRUE
                LEFT JOIN TGT.DWH_D_SUBCATEGORY_LU TGT ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
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
