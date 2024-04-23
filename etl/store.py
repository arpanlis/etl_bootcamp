from base_etl import BaseETL
from con import get_connection


class StoreETL(BaseETL):
    table = "STORE"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
    create table if not exists STG.STG_D_STORE_LU (
        ID NUMBER(38,0) NOT NULL,
        REGION_ID NUMBER(38,0),
        STORE_DESC VARCHAR(256),
        primary key (ID),
        foreign key (REGION_ID) references BHATBHATENI.TRANSACTIONS.REGION(ID)
    );
    """

    temp_ddl = """
    create table if not exists TMP.TMP_D_STORE_LU (
        ID NUMBER(38,0) NOT NULL,
        REGION_ID NUMBER(38,0),
        STORE_DESC VARCHAR(256),
        PRIMARY KEY (ID),
        FOREIGN KEY (REGION_ID) REFERENCES TMP.TMP_D_REGION_LU(ID)
    );
    """

    target_ddl = """
    create table if not exists TGT.DWH_D_STORE_LU (
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        REGION_ID_SK NUMBER(38,0),
        STORE_DESC VARCHAR(256),
        VALID_FROM TIMESTAMP,
        VALID_TILL TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
        primary key (ID_SK),
        FOREIGN KEY (REGION_ID_SK) REFERENCES TGT.DWH_D_REGION_LU(ID_SK)
    );
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Insert new records
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, REGION_ID_SK, STORE_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT SRC.ID, RGN.ID_SK, SRC.STORE_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM TMP.{self.TEMP_TABLE} SRC
            LEFT JOIN TGT.DWH_D_REGION_LU RGN ON SRC.REGION_ID = RGN.SOURCE_ID
            WHERE SRC.ID NOT IN (SELECT SOURCE_ID FROM {self.TARGET_TABLE})
            """
        )

        # Update existing records (for minor changes)
        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING (
                SELECT SRC.ID, SRC.STORE_DESC, SRC.REGION_ID, RGN.ID_SK AS REGION_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_REGION_LU RGN ON SRC.REGION_ID = RGN.SOURCE_ID
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID
            WHEN MATCHED AND (
                TGT.STORE_DESC <> SRC.STORE_DESC
                OR TGT.REGION_ID_SK <> SRC.REGION_ID_SK
            ) THEN
            UPDATE
            SET TGT.STORE_DESC = SRC.STORE_DESC,
                TGT.REGION_ID_SK = SRC.REGION_ID_SK,
                TGT.VALID_TILL = CURRENT_TIMESTAMP(),
                TGT.ACTIVE_FLAG = FALSE
            """
        )

        # Insert updated records as new rows
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, REGION_ID_SK, STORE_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT SRC.ID, SRC.REGION_ID_SK, SRC.STORE_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM (
                SELECT SRC.ID, SRC.STORE_DESC, SRC.REGION_ID, RGN.ID_SK AS REGION_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_REGION_LU RGN ON SRC.REGION_ID = RGN.SOURCE_ID
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
