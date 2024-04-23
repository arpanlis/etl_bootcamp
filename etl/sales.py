from base_etl import BaseETL
from con import get_connection


class SalesETL(BaseETL):
    table = "SALES"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
    create table if not exists STG.STG_F_SALES_TRXN_B (
        id NUMBER,
        store_id NUMBER NOT NULL,
        product_id NUMBER NOT NULL,
        customer_id NUMBER,
        transaction_time TIMESTAMP,
        quantity NUMBER,
        amount NUMBER(20,2),
        discount NUMBER(20,2),
        primary key (id),
        FOREIGN KEY (store_id) references STG.STG_D_STORE_LU(id),
        FOREIGN KEY (product_id) references STG.STG_D_PRODUCT_LU(id),
        FOREIGN KEY (customer_id) references STG.STG_D_CUSTOMER_LU(id)
    );
    """

    temp_ddl = """
    create table if not exists TMP.TMP_F_SALES_TRXN_B (
        ID NUMBER,
        STORE_ID NUMBER NOT NULL,
        PRODUCT_ID NUMBER NOT NULL,
        CUSTOMER_ID NUMBER,
        TRANSACTION_TIME TIMESTAMP,
        QUANTITY NUMBER,
        AMOUNT NUMBER(20,2),
        DISCOUNT NUMBER(20,2),
        PRIMARY KEY (ID),
        FOREIGN KEY (STORE_ID) REFERENCES TMP.TMP_D_STORE_LU(ID),
        FOREIGN KEY (PRODUCT_ID) REFERENCES TMP.TMP_D_PRODUCT_LU(ID),
        FOREIGN KEY (CUSTOMER_ID) REFERENCES TMP.TMP_D_CUSTOMER_LU(ID)
    );
    """

    target_ddl = """
    create table if not exists TGT.DWH_F_SALES_TRXN_B (
        ID_SK INT AUTOINCREMENT,
        SOURCE_ID NUMBER,
        STORE_ID_SK NUMBER(38,0) NOT NULL,
        PRODUCT_ID_SK NUMBER(38,0) NOT NULL,
        CUSTOMER_ID_SK NUMBER(38,0),
        TRANSACTION_TIME TIMESTAMP,
        QUANTITY NUMBER,
        AMOUNT NUMBER(20,2),
        DISCOUNT NUMBER(20,2),
        LOAD_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ID_SK),
        FOREIGN KEY (STORE_ID_SK) REFERENCES TGT.DWH_D_STORE_LU(ID_SK),
        FOREIGN KEY (PRODUCT_ID_SK) REFERENCES TGT.DWH_D_PRODUCT_LU(ID_SK),
        FOREIGN KEY (CUSTOMER_ID_SK) REFERENCES TGT.DWH_D_CUSTOMER_LU(ID_SK)
    );
    """

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        # Truncate target table
        cur.execute(f"TRUNCATE TABLE {self.TARGET_TABLE}")

        # Insert new records
        cur.execute(
            f"""
            INSERT INTO {self.TARGET_TABLE} (SOURCE_ID, STORE_ID_SK, PRODUCT_ID_SK, CUSTOMER_ID_SK, TRANSACTION_TIME, QUANTITY, AMOUNT, DISCOUNT)
            SELECT SRC.ID, STR.ID_SK, PRD.ID_SK, CUS.ID_SK, SRC.TRANSACTION_TIME, SRC.QUANTITY, SRC.AMOUNT, SRC.DISCOUNT
            FROM TMP.{self.TEMP_TABLE} SRC
            LEFT JOIN TGT.DWH_D_STORE_LU STR ON SRC.STORE_ID = STR.SOURCE_ID
            LEFT JOIN TGT.DWH_D_PRODUCT_LU PRD ON SRC.PRODUCT_ID = PRD.SOURCE_ID
            LEFT JOIN TGT.DWH_D_CUSTOMER_LU CUS ON SRC.CUSTOMER_ID = CUS.SOURCE_ID
            WHERE SRC.ID NOT IN (SELECT SOURCE_ID FROM {self.TARGET_TABLE})
            """
        )

        # Update existing records (for minor changes)
        cur.execute(
            f"""
            MERGE INTO {self.TARGET_TABLE} TGT
            USING (
                SELECT SRC.ID, SRC.TRANSACTION_TIME, SRC.QUANTITY, SRC.AMOUNT, SRC.DISCOUNT,
                    STR.ID_SK AS STORE_ID_SK, PRD.ID_SK AS PRODUCT_ID_SK, CUS.ID_SK AS CUSTOMER_ID_SK
                FROM TMP.{self.TEMP_TABLE} SRC
                LEFT JOIN TGT.DWH_D_STORE_LU STR ON SRC.STORE_ID = STR.SOURCE_ID
                LEFT JOIN TGT.DWH_D_PRODUCT_LU PRD ON SRC.PRODUCT_ID = PRD.SOURCE_ID
                LEFT JOIN TGT.DWH_D_CUSTOMER_LU CUS ON SRC.CUSTOMER_ID = CUS.SOURCE_ID
            ) SRC
            ON TGT.SOURCE_ID = SRC.ID
            WHEN MATCHED AND (
                TGT.TRANSACTION_TIME <> SRC.TRANSACTION_TIME
                OR TGT.QUANTITY <> SRC.QUANTITY
                OR TGT.AMOUNT <> SRC.AMOUNT
                OR TGT.DISCOUNT <> SRC.DISCOUNT
                OR TGT.STORE_ID_SK <> SRC.STORE_ID_SK
                OR TGT.PRODUCT_ID_SK <> SRC.PRODUCT_ID_SK
                OR TGT.CUSTOMER_ID_SK <> SRC.CUSTOMER_ID_SK
            ) THEN
            UPDATE
            SET TGT.TRANSACTION_TIME = SRC.TRANSACTION_TIME,
                TGT.QUANTITY = SRC.QUANTITY,
                TGT.AMOUNT = SRC.AMOUNT,
                TGT.DISCOUNT = SRC.DISCOUNT,
                TGT.STORE_ID_SK = SRC.STORE_ID_SK,
                TGT.PRODUCT_ID_SK = SRC.PRODUCT_ID_SK,
                TGT.CUSTOMER_ID_SK = SRC.CUSTOMER_ID_SK
            """
        )

        print(f"Data has been loaded into {self.TARGET_TABLE}")
        cur.close()
