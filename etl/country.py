from base_etl import BaseETL


class CountryETL(BaseETL):
    table = "COUNTRY"
    schema = "TRANSACTIONS"


country_etl = CountryETL()
