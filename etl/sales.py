from core.base_etl import BaseETL
from core.connection import get_connection


class SalesETL(BaseETL):
    table = "SALES"
    source_schema = "TRANSACTIONS"

    staging_ddl = """
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
    """

    temp_ddl = """
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
    """

    target_ddl = """
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
    """

    tgt_sales_agg_ddl = """
        ID_SK INT AUTOINCREMENT,
        MONTH VARCHAR,
        STORE_ID_SK INT,
        AMOUNT NUMBER,
        DISCOUNT NUMBER,
        PRIMARY KEY (ID_SK),
        FOREIGN KEY (STORE_ID_SK) REFERENCES TGT.DWH_D_STORE_LU(ID_SK)
    """

    def __init__(self) -> None:
        super().__init__()

        # Create sales aggregation table
        self.run_ddl(
            table="DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T",
            ddl=self.tgt_sales_agg_ddl,
            schema="TGT",
        )

    def load(self):
        conn = get_connection(schema="TGT")
        cur = conn.cursor()

        cur.execute(
            f"""
            MERGE INTO TGT.{self.TARGET_TABLE} F
            USING (
                SELECT
                    S.ID AS SOURCE_ID,
                    ST.ID_SK AS STORE_ID_SK,
                    P.ID_SK AS PRODUCT_ID_SK,
                    C.ID_SK AS CUSTOMER_ID_SK,
                    S.TRANSACTION_TIME,
                    S.QUANTITY,
                    S.AMOUNT,
                    S.DISCOUNT
                FROM TMP.{self.TEMP_TABLE} S
                JOIN TGT.DWH_D_STORE_LU ST ON ST.SOURCE_ID = S.STORE_ID AND ST.ACTIVE_FLAG = TRUE
                JOIN TGT.DWH_D_PRODUCT_LU P ON P.SOURCE_ID = S.PRODUCT_ID AND P.ACTIVE_FLAG = TRUE
                LEFT JOIN TGT.DWH_D_CUSTOMER_LU C ON C.SOURCE_ID = S.CUSTOMER_ID AND C.ACTIVE_FLAG = TRUE
            ) SRC
            ON F.SOURCE_ID = SRC.SOURCE_ID
            WHEN MATCHED THEN
                UPDATE
                SET F.STORE_ID_SK = SRC.STORE_ID_SK,
                    F.PRODUCT_ID_SK = SRC.PRODUCT_ID_SK,
                    F.CUSTOMER_ID_SK = SRC.CUSTOMER_ID_SK,
                    F.TRANSACTION_TIME = SRC.TRANSACTION_TIME,
                    F.QUANTITY = SRC.QUANTITY,
                    F.AMOUNT = SRC.AMOUNT,
                    F.DISCOUNT = SRC.DISCOUNT
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ID, STORE_ID_SK, PRODUCT_ID_SK, CUSTOMER_ID_SK, TRANSACTION_TIME, QUANTITY, AMOUNT, DISCOUNT)
                VALUES (SRC.SOURCE_ID, SRC.STORE_ID_SK, SRC.PRODUCT_ID_SK, SRC.CUSTOMER_ID_SK, SRC.TRANSACTION_TIME, SRC.QUANTITY, SRC.AMOUNT, SRC.DISCOUNT);
            """
        )

        # Aggregate sales data by month and store
        cur.execute(
            f"""
            INSERT INTO TGT.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T (MONTH, STORE_ID_SK, AMOUNT, DISCOUNT)
            SELECT
                TO_CHAR(S.TRANSACTION_TIME, 'YYYY-MM'),
                ST.ID_SK as STORE_ID_SK,
                SUM(S.AMOUNT),
                SUM(S.DISCOUNT)
            FROM TMP.{self.TEMP_TABLE} S
            JOIN TGT.DWH_D_STORE_LU ST ON ST.SOURCE_ID = S.STORE_ID AND ST.ACTIVE_FLAG = TRUE
            GROUP BY TO_CHAR(S.TRANSACTION_TIME, 'YYYY-MM'), ST.ID_SK;
            """
        )

        conn.commit()
        cur.close()

        print(f"Data has been loaded into {self.TARGET_TABLE}")
