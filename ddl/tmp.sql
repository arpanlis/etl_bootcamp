-- Temp COUNTRY
create or replace TABLE TMP.TMP_D_COUNTRY_LU (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_DESC VARCHAR(256),
	primary key (ID)
);

-- Temp REGION
create or replace TABLE TMP.TMP_D_REGION_LU (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_ID NUMBER(38,0),
	REGION_DESC VARCHAR(256),
	primary key (ID),
	foreign key (COUNTRY_ID) references BHATBHATENI.TRANSACTIONS.COUNTRY(ID)
);

-- Temp STORE
create or replace TABLE TMP.TMP_D_STORE_LU (
	ID NUMBER(38,0) NOT NULL,
	REGION_ID NUMBER(38,0),
	STORE_DESC VARCHAR(256),
	primary key (ID),
	foreign key (REGION_ID) references BHATBHATENI.TRANSACTIONS.REGION(ID)
);

-- Temp CUSTOMER
create or replace TABLE TMP.TMP_D_CUSTOMER_LU (
	ID NUMBER(38,0) NOT NULL,
	CUSTOMER_FIRST_NAME VARCHAR(256),
	CUSTOMER_MIDDLE_NAME VARCHAR(256),
	CUSTOMER_LAST_NAME VARCHAR(256),
	CUSTOMER_ADDRESS VARCHAR(256),
	primary key (ID)
);

-- Temp TRANSACTION
create or replace TABLE TMP.TMP_D_CATEGORY_LU (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_DESC VARCHAR(1024),
	primary key (ID)
);

-- Temp TRANSACTION
create or replace TABLE TMP.TMP_D_SUBCATEGORY_LU (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_ID NUMBER(38,0),
	SUBCATEGORY_DESC VARCHAR(256),
	primary key (ID),
	foreign key (CATEGORY_ID) references BHATBHATENI.TRANSACTIONS.CATEGORY(ID)
);

-- Temp PRODUCT
create or replace TABLE TMP.TMP_D_PRODUCT_LU (
	ID NUMBER(38,0) NOT NULL,
	SUBCATEGORY_ID NUMBER(38,0),
	PRODUCT_DESC VARCHAR(256),
	primary key (ID),
	foreign key (SUBCATEGORY_ID) references BHATBHATENI.TRANSACTIONS.SUBCATEGORY(ID)
);