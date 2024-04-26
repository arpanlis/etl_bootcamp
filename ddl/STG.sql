-- Staging COUNTRY
create table if not exists STG.STG_D_COUNTRY_LU (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_DESC VARCHAR(256),
	primary key (ID)
);

-- Staging REGION
create table if not exists STG.STG_D_REGION_LU (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_ID NUMBER(38,0),
	REGION_DESC VARCHAR(256),
	primary key (ID),
	foreign key (COUNTRY_ID) references BHATBHATENI.TRANSACTIONS.COUNTRY(ID)
);

-- Staging STORE
create table if not exists STG.STG_D_STORE_LU (
	ID NUMBER(38,0) NOT NULL,
	REGION_ID NUMBER(38,0),
	STORE_DESC VARCHAR(256),
	primary key (ID),
	foreign key (REGION_ID) references BHATBHATENI.TRANSACTIONS.REGION(ID)
);

-- Staging CUSTOMER
create table if not exists STG.STG_D_CUSTOMER_LU (
	ID NUMBER(38,0) NOT NULL,
	CUSTOMER_FIRST_NAME VARCHAR(256),
	CUSTOMER_MIDDLE_NAME VARCHAR(256),
	CUSTOMER_LAST_NAME VARCHAR(256),
	CUSTOMER_ADDRESS VARCHAR(256),
	primary key (ID)
);

-- Staging TRANSACTION
create table if not exists STG.STG_D_CATEGORY_LU (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_DESC VARCHAR(1024),
	primary key (ID)
);

-- Staging TRANSACTION
create table if not exists STG.STG_D_SUBCATEGORY_LU (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_ID NUMBER(38,0),
	SUBCATEGORY_DESC VARCHAR(256),
	primary key (ID),
	foreign key (CATEGORY_ID) references BHATBHATENI.TRANSACTIONS.CATEGORY(ID)
);

-- Staging PRODUCT
create table if not exists STG.STG_D_PRODUCT_LU (
	ID NUMBER(38,0) NOT NULL,
	SUBCATEGORY_ID NUMBER(38,0),
	PRODUCT_DESC VARCHAR(256),
	primary key (ID),
	foreign key (SUBCATEGORY_ID) references BHATBHATENI.TRANSACTIONS.SUBCATEGORY(ID)
);


-- Staging SALES
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
    FOREIGN KEY (store_id) references STG_D_STORE_LU(id),
    FOREIGN KEY (product_id) references STG_D_PRODUCT_LU(id),
    FOREIGN KEY (customer_id) references STG_D_CUSTOMER_LU(id)
);
