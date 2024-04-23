from base_etl import BaseETL
from connection import get_connection


class RegionETL(BaseETL):

    table = "REGION"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
    create table if not exists STG.STG_D_REGION_LU (
        ID NUMBER(38,0) NOT NULL,
        COUNTRY_ID NUMBER(38,0),
        REGION_DESC VARCHAR(256),
        primary key (ID),
        foreign key (COUNTRY_ID) references BHATBHATENI.TRANSACTIONS.COUNTRY(ID)
    );
    """

    temp_ddl = """
    create table if not exists TMP.TMP_D_REGION_LU (
        ID NUMBER(38,0) NOT NULL,
        COUNTRY_ID NUMBER(38,0),
        REGION_DESC VARCHAR(256),
        PRIMARY KEY (ID),
        FOREIGN KEY (COUNTRY_ID) REFERENCES TMP.TMP_D_COUNTRY_LU(ID)
    );
    """

    target_ddl = """
    create table if not exists TGT.DWH_D_REGION_LU (
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER(38,0) NOT NULL,
        COUNTRY_ID_SK NUMBER(38,0),
        REGION_DESC VARCHAR(256),
        VALID_FROM TIMESTAMP,
        VALID_TILL TIMESTAMP,
        ACTIVE_FLAG BOOLEAN,
        primary key (ID_SK),
        FOREIGN KEY (COUNTRY_ID_SK) REFERENCES TGT.DWH_D_COUNTRY_LU(ID_SK)
    );
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Insert new records
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, COUNTRY_ID_SK, REGION_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT ID, COUNTRY_ID, REGION_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM TMP.{self.TEMP_TABLE} TMP
            JOIN TGT.DWH_D_COUNTRY_LU TGT ON TMP.COUNTRY_ID = TGT.SOURCE_ID
            WHERE TMP.ID NOT IN (SELECT SOURCE_ID FROM {self.TARGET_TABLE})
            """
        )

        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING (
                SELECT SRC.ID, SRC.REGION_DESC, SRC.COUNTRY_ID, CTY.ID_SK AS COUNTRY_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_COUNTRY_LU CTY ON SRC.COUNTRY_ID = CTY.SOURCE_ID
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID
            WHEN MATCHED AND (
                TGT.REGION_DESC <> SRC.REGION_DESC
                OR TGT.COUNTRY_ID_SK <> SRC.COUNTRY_ID_SK
            ) THEN
            UPDATE
            SET TGT.REGION_DESC = SRC.REGION_DESC,
                TGT.COUNTRY_ID_SK = SRC.COUNTRY_ID_SK,
                TGT.VALID_TILL = CURRENT_TIMESTAMP(),
                TGT.ACTIVE_FLAG = FALSE
            """
        )

        print(f"Data has been loaded into {self.TARGET_TABLE}")
        cur.close()
