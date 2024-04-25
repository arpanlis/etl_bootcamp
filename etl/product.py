from base_etl import BaseETL
from connection import get_connection


class ProductETL(BaseETL):
    table = "PRODUCT"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
        ID NUMBER(38,0) NOT NULL,
        SUBCATEGORY_ID NUMBER(38,0),
        PRODUCT_DESC VARCHAR(256),
        primary key (ID),
        foreign key (SUBCATEGORY_ID) references BHATBHATENI.TRANSACTIONS.SUBCATEGORY(ID)
    """

    temp_ddl = """
        ID NUMBER(38,0) NOT NULL,
        SUBCATEGORY_ID NUMBER(38,0),
        PRODUCT_DESC VARCHAR(256),
        PRIMARY KEY (ID),
        FOREIGN KEY (SUBCATEGORY_ID) REFERENCES TMP.TMP_D_SUBCATEGORY_LU(ID)
    """

    target_ddl = """
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        SUBCATEGORY_ID_SK NUMBER(38,0),
        PRODUCT_DESC VARCHAR(256),
        START_DATE TIMESTAMP,
        CLOSE_DATE TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
        primary key (ID_SK),
        FOREIGN KEY (SUBCATEGORY_ID_SK) REFERENCES TGT.DWH_D_SUBCATEGORY_LU(ID_SK)
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
            MERGE INTO TGT.DWH_D_PRODUCT_LU TGT
            USING (
                SELECT
                    SRC.ID,
                    SRC.SUBCATEGORY_ID,
                    SRC.PRODUCT_DESC,
                    SUBCATEGORY.ID_SK AS SUBCATEGORY_ID_SK
                FROM TMP.TMP_D_PRODUCT_LU SRC
                LEFT JOIN TGT.DWH_D_SUBCATEGORY_LU SUBCATEGORY ON SUBCATEGORY.SOURCE_ID = SRC.SUBCATEGORY_ID AND SUBCATEGORY.ACTIVE_FLAG = TRUE
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
            WHEN MATCHED AND (TGT.PRODUCT_DESC <> SRC.PRODUCT_DESC)
            THEN
                UPDATE
                SET TGT.PRODUCT_DESC = SRC.PRODUCT_DESC,
                    TGT.SUBCATEGORY_ID_SK = SRC.SUBCATEGORY_ID_SK,
                    TGT.START_DATE = CURRENT_TIMESTAMP(),
                    TGT.ACTIVE_FLAG = TRUE
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ID, SUBCATEGORY_ID_SK, PRODUCT_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                VALUES (SRC.ID, SRC.SUBCATEGORY_ID_SK, SRC.PRODUCT_DESC, CURRENT_TIMESTAMP(), NULL, TRUE);
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
                    MERGE INTO TGT.DWH_D_PRODUCT_LU TGT
                    USING (
                        SELECT
                            SRC.ID,
                            SRC.SUBCATEGORY_ID,
                            SRC.PRODUCT_DESC,
                            SUBCATEGORY.ID_SK AS SUBCATEGORY_ID_SK
                        FROM TMP.TMP_D_PRODUCT_LU SRC
                        LEFT JOIN TGT.DWH_D_SUBCATEGORY_LU SUBCATEGORY ON SUBCATEGORY.SOURCE_ID = SRC.SUBCATEGORY_ID AND SUBCATEGORY.ACTIVE_FLAG = TRUE
                    ) SRC
                    ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
                    WHEN MATCHED AND TGT.SUBCATEGORY_ID_SK <> SRC.SUBCATEGORY_ID_SK
                    THEN
                        UPDATE
                        SET TGT.CLOSE_DATE = CURRENT_TIMESTAMP(),
                            TGT.ACTIVE_FLAG = FALSE
                """
            )

            cur.execute(
                """
                INSERT INTO TGT.DWH_D_PRODUCT_LU (SOURCE_ID, SUBCATEGORY_ID_SK, PRODUCT_DESC, START_DATE, CLOSE_DATE, ACTIVE_FLAG)
                SELECT
                    SRC.ID,
                    SUBCAT.ID_SK,
                    SRC.PRODUCT_DESC,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    TRUE
                FROM TMP.TMP_D_PRODUCT_LU SRC
                JOIN TGT.DWH_D_SUBCATEGORY_LU SUBCAT ON SUBCAT.SOURCE_ID = SRC.SUBCATEGORY_ID AND SUBCAT.ACTIVE_FLAG = TRUE
                LEFT JOIN TGT.DWH_D_PRODUCT_LU TGT ON TGT.SOURCE_ID = SRC.ID AND TGT.ACTIVE_FLAG = TRUE
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
