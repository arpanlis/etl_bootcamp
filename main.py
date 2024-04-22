from constants import CUSTOMER_HIERARCHY, LOCATION_HIERARCHY, PRODUCT_HIERARCHY
from etl.category import etl_category
from etl.country import country_etl
from etl.customer import etl_customer
from etl.product import etl_product
from etl.region import etl_region
from etl.sales import etl_sales
from etl.store import etl_store
from etl.subcategory import etl_subcategory
from helpers import export_to_local_csv, schema_and_table_init

# if __name__ == "__main__":
#     schema_and_table_init()
#
#     # Export Data from Location Hierarchy table in file format
#     for table in LOCATION_HIERARCHY:
#         export_to_local_csv(schema="TRANSACTIONS", table=table)
#
#     # Export other dimension tables
#     for table in PRODUCT_HIERARCHY + CUSTOMER_HIERARCHY:
#         export_to_local_csv(schema="TRANSACTIONS", table=table)
#
#     # Export sales fact table
#     export_to_local_csv(schema="TRANSACTIONS", table="SALES")
#
#     etl_country()
#     etl_region()
#     etl_store()
#     etl_category()
#     etl_subcategory()
#     etl_product()
#     etl_customer()
#     etl_sales()

if __name__ == "__main__":
    country_etl.run()
