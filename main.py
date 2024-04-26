from etl import (
    CategoryETL,
    CountryETL,
    CustomerETL,
    ProductETL,
    RegionETL,
    SalesETL,
    StoreETL,
    SubcategoryETL,
)

if __name__ == "__main__":
    CountryETL().run()
    RegionETL().run()
    StoreETL().run()
    CategoryETL().run()
    SubcategoryETL().run()
    ProductETL().run()
    CustomerETL().run()
    SalesETL().run()
