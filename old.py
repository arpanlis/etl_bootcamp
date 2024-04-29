from snowflake.snowpark import Session
import os

from con import get_session
from constants import (
    CUSTOMER_HIERARCHY,
    DIMENSION_TABLES,
    FACT_TABLES,
    SCHEMAS,
    LOCATION_HIERARCHY,
    PRODUCT_HIERARCHY,
    SOURCE_SCHEMA,
)


def check_if_schema_exist(sess: Session):
    for schema in SCHEMAS:
        sess.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def ddl_source_to_stg(sess: Session):
    if not os.path.exists("./ddl"):
        os.makedirs("./ddl")

    stg_sess = get_session(schema="STG")

    for table in DIMENSION_TABLES:
        res = sess.sql(f"SELECT GET_DDL('TABLE', '{SOURCE_SCHEMA}.{table}') AS DDL")
        df = res.to_pandas()
        raw_ddl = df["DDL"][0]

        res = stg_sess.sql(raw_ddl)  # type: ignore


def table_to_file(sess: Session, tables: list[str]):
    if not os.path.exists("./data"):
        os.makedirs("./data")

    for table in tables:
        res = sess.sql(f"SELECT * FROM {SOURCE_SCHEMA}.{table}")

        pandas_df = res.to_pandas()

        pandas_df.to_csv(f"./data/{table}.csv", index=False)


def create_schemas(sess: Session):
    for schema in SCHEMAS:
        sess.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        sess.sql("""
                 CREATE OR REPLACE TABLE STG.PRODUCT_HIERARCHY ()
                 """)


def create_dimension_tables(sess: Session):
    for schema in SCHEMAS:
        for table in DIMENSION_TABLES:
            sess.sql(
                f"CREATE TABLE IF NOT EXISTS {schema}.{table} (ID STRING, NAME STRING, DESCRIPTION STRING)"
            )


if __name__ == "__main__":
    sess = get_session(schema="TRANSACTIONS")

    ddl_source_to_stg(sess)

    check_if_schema_exist(sess)

    table_to_file(sess, tables=LOCATION_HIERARCHY)

    table_to_file(sess, tables=PRODUCT_HIERARCHY)

    table_to_file(sess, tables=CUSTOMER_HIERARCHY)

    table_to_file(sess, tables=FACT_TABLES)
