from base_etl import BaseETL
from con import get_connection


class ProductETL(BaseETL):
    table = "PRODUCT"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
    create table if not exists STG.STG_D_PRODUCT_LU (
        ID NUMBER(38,0) NOT NULL,
        SUBCATEGORY_ID NUMBER(38,0),
        PRODUCT_DESC VARCHAR(256),
        primary key (ID),
        foreign key (SUBCATEGORY_ID) references BHATBHATENI.TRANSACTIONS.SUBCATEGORY(ID)
    );
    """

    temp_ddl = """
    create table if not exists TMP.TMP_D_PRODUCT_LU (
        ID NUMBER(38,0) NOT NULL,
        SUBCATEGORY_ID NUMBER(38,0),
        PRODUCT_DESC VARCHAR(256),
        PRIMARY KEY (ID),
        FOREIGN KEY (SUBCATEGORY_ID) REFERENCES TMP.TMP_D_SUBCATEGORY_LU(ID)
    );
    """

    target_ddl = """
    create table if not exists TGT.DWH_D_PRODUCT_LU (
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        SUBCATEGORY_ID_SK NUMBER(38,0),
        PRODUCT_DESC VARCHAR(256),
        VALID_FROM TIMESTAMP,
        VALID_TILL TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
        primary key (ID_SK),
        FOREIGN KEY (SUBCATEGORY_ID_SK) REFERENCES TGT.DWH_D_SUBCATEGORY_LU(ID_SK)
    );
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Insert new records
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, SUBCATEGORY_ID_SK, PRODUCT_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT SRC.ID, SUB.ID_SK, SRC.PRODUCT_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM TMP.{self.TEMP_TABLE} SRC
            LEFT JOIN TGT.DWH_D_SUBCATEGORY_LU SUB ON SRC.SUBCATEGORY_ID = SUB.SOURCE_ID
            WHERE SRC.ID NOT IN (SELECT SOURCE_ID FROM {self.TARGET_TABLE})
            """
        )

        # Update existing records (for minor changes)
        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING (
                SELECT SRC.ID, SRC.PRODUCT_DESC, SRC.SUBCATEGORY_ID, SUB.ID_SK AS SUBCATEGORY_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_SUBCATEGORY_LU SUB ON SRC.SUBCATEGORY_ID = SUB.SOURCE_ID
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID
            WHEN MATCHED AND (
                TGT.PRODUCT_DESC <> SRC.PRODUCT_DESC
                OR TGT.SUBCATEGORY_ID_SK <> SRC.SUBCATEGORY_ID_SK
            ) THEN
            UPDATE
            SET TGT.PRODUCT_DESC = SRC.PRODUCT_DESC,
                TGT.SUBCATEGORY_ID_SK = SRC.SUBCATEGORY_ID_SK,
                TGT.VALID_TILL = CURRENT_TIMESTAMP(),
                TGT.ACTIVE_FLAG = FALSE
            """
        )

        # Insert updated records as new rows
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, SUBCATEGORY_ID_SK, PRODUCT_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT SRC.ID, SRC.SUBCATEGORY_ID_SK, SRC.PRODUCT_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM (
                SELECT SRC.ID, SRC.PRODUCT_DESC, SRC.SUBCATEGORY_ID, SUB.ID_SK AS SUBCATEGORY_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_SUBCATEGORY_LU SUB ON SRC.SUBCATEGORY_ID = SUB.SOURCE_ID
            ) SRC
            WHERE SRC.ID IN (
                SELECT SOURCE_ID
                FROM {self.TARGET_TABLE}
                WHERE VALID_TILL = CURRENT_TIMESTAMP()
            )
            """
        )

        print(f"Data has been loaded into {self.TARGET_TABLE}")
        cur.close()
