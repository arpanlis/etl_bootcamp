from base_etl import BaseETL
from connection import get_connection


class SubcategoryETL(BaseETL):
    table = "SUBCATEGORY"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
    create table if not exists STG.STG_D_SUBCATEGORY_LU (
        ID NUMBER(38,0) NOT NULL,
        CATEGORY_ID NUMBER(38,0),
        SUBCATEGORY_DESC VARCHAR(256),
        primary key (ID),
        foreign key (CATEGORY_ID) references BHATBHATENI.TRANSACTIONS.CATEGORY(ID)
    );
    """

    temp_ddl = """
    create table if not exists TMP.TMP_D_SUBCATEGORY_LU (
        ID NUMBER(38,0) NOT NULL,
        CATEGORY_ID NUMBER(38,0),
        SUBCATEGORY_DESC VARCHAR(256),
        PRIMARY KEY (ID),
        FOREIGN KEY (CATEGORY_ID) REFERENCES TMP.TMP_D_CATEGORY_LU(ID)
    );
    """

    target_ddl = """
    create table if not exists TGT.DWH_D_SUBCATEGORY_LU (
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        CATEGORY_ID_SK NUMBER(38,0),
        SUBCATEGORY_DESC VARCHAR(256),
        VALID_FROM TIMESTAMP,
        VALID_TILL TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
        primary key (ID_SK),
        FOREIGN KEY (CATEGORY_ID_SK) REFERENCES TGT.DWH_D_CATEGORY_LU(ID_SK)
    );
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Insert new records
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, CATEGORY_ID_SK, SUBCATEGORY_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT SRC.ID, CAT.ID_SK, SRC.SUBCATEGORY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM TMP.{self.TEMP_TABLE} SRC
            LEFT JOIN TGT.DWH_D_CATEGORY_LU CAT ON SRC.CATEGORY_ID = CAT.SOURCE_ID
            WHERE SRC.ID NOT IN (SELECT SOURCE_ID FROM {self.TARGET_TABLE})
            """
        )

        # Update existing records (for minor changes)
        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING (
                SELECT SRC.ID, SRC.SUBCATEGORY_DESC, SRC.CATEGORY_ID, CAT.ID_SK AS CATEGORY_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_CATEGORY_LU CAT ON SRC.CATEGORY_ID = CAT.SOURCE_ID
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID
            WHEN MATCHED AND (
                TGT.SUBCATEGORY_DESC <> SRC.SUBCATEGORY_DESC
                OR TGT.CATEGORY_ID_SK <> SRC.CATEGORY_ID_SK
            ) THEN
            UPDATE
            SET TGT.SUBCATEGORY_DESC = SRC.SUBCATEGORY_DESC,
                TGT.CATEGORY_ID_SK = SRC.CATEGORY_ID_SK,
                TGT.VALID_TILL = CURRENT_TIMESTAMP(),
                TGT.ACTIVE_FLAG = FALSE
            """
        )

        # Insert updated records as new rows
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, CATEGORY_ID_SK, SUBCATEGORY_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT SRC.ID, SRC.CATEGORY_ID_SK, SRC.SUBCATEGORY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM (
                SELECT SRC.ID, SRC.SUBCATEGORY_DESC, SRC.CATEGORY_ID, CAT.ID_SK AS CATEGORY_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_CATEGORY_LU CAT ON SRC.CATEGORY_ID = CAT.SOURCE_ID
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
