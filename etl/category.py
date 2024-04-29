from snowflake.snowpark import Session

from con import get_session

CATEGORY_DDL = """
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_DESC VARCHAR(1024),
	primary key (ID)
"""


def category_source_to_stg():
    sess = get_session()
    res = sess.sql("""
             COPY INTO STG.CATEGORY FROM @STAGE.BHATBHATENI.CATEGORY FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"') ON_ERROR=CONTINUE
     """)
    res.show()


def category_stg_to_tmp(sess: Session):
    pass


def category_tmp_to_tgt(sess: Session):
    pass
