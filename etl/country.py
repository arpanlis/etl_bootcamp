from base_etl import BaseETL
from con import get_connection


class CountryETL(BaseETL):
    table = "COUNTRY"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
        create table if not exists STG.STG_D_COUNTRY_LU (
            ID NUMBER(38,0) NOT NULL,
            COUNTRY_DESC VARCHAR(256),
            primary key (ID)
    );
    """

    temp_ddl = """
        create table if not exists TMP.TMP_D_COUNTRY_LU (
            ID NUMBER(38,0) NOT NULL,
            COUNTRY_DESC VARCHAR(256),
            PRIMARY KEY (ID)
        );
    """

    target_ddl = """
        create table if not exists TGT.DWH_D_COUNTRY_LU (
            ID_SK INT AUTOINCREMENT,
            SOURCE_ID NUMBER(38,0) NOT NULL,
            COUNTRY_DESC VARCHAR(256),
            VALID_FROM TIMESTAMP,
            VALID_TILL TIMESTAMP,
            ACTIVE_FLAG BOOLEAN,
            primary key (ID_SK)
        );
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Insert new records
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, COUNTRY_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT ID, COUNTRY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM TMP.{self.TEMP_TABLE}
            WHERE ID NOT IN (SELECT SOURCE_ID FROM {self.TARGET_TABLE})
        """
        )

        # Update existing records (for minor changes)
        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING TMP.{self.TEMP_TABLE} SRC
            ON TGT.SOURCE_ID = SRC.ID
            WHEN MATCHED AND TGT.COUNTRY_DESC <> SRC.COUNTRY_DESC THEN
                UPDATE
                SET TGT.COUNTRY_DESC = SRC.COUNTRY_DESC,
                    TGT.VALID_TILL = CURRENT_TIMESTAMP(),
                    TGT.ACTIVE_FLAG = FALSE
        """
        )

        # Insert updated records as new rows
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, COUNTRY_DESC, VALID_FROM, VALID_TILL, ACTIVE_FLAG)
            SELECT ID, COUNTRY_DESC, CURRENT_TIMESTAMP(), NULL, TRUE
            FROM TMP.{self.TEMP_TABLE}
            WHERE ID IN (
                SELECT SOURCE_ID
                FROM {self.TARGET_TABLE}
                WHERE VALID_TILL = CURRENT_TIMESTAMP()
            )
        """
        )

        print(f"Data has been loaded into {self.TARGET_TABLE}")

        cur.close()
