from snowflake.snowpark import Session
import os

from etl.category import category_source_to_stg

if __name__ == "__main__":
   category_source_to_stg()
