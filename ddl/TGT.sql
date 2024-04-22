-- Target COUNTRY
create table if not exists TGT.DWH_D_COUNTRY_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    COUNTRY_DESC VARCHAR(256),
    PRIMARY KEY (ID_SK)
);

-- Target REGION
create table if not exists TGT.DWH_D_REGION_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    COUNTRY_ID_SK NUMBER(38,0),
    REGION_DESC VARCHAR(256),
    PRIMARY KEY (ID_SK),
    FOREIGN KEY (COUNTRY_ID_SK) REFERENCES TGT.DWH_D_COUNTRY_LU(ID_SK)
);

-- Target STORE
create table if not exists TGT.DWH_D_STORE_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    REGION_ID_SK NUMBER(38,0),
    STORE_DESC VARCHAR(256),
    PRIMARY KEY (ID_SK),
    FOREIGN KEY (REGION_ID_SK) REFERENCES TGT.DWH_D_REGION_LU(ID_SK)
);

-- Target CUSTOMER
create table if not exists TGT.DWH_D_CUSTOMER_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    CUSTOMER_FIRST_NAME VARCHAR(256),
    CUSTOMER_MIDDLE_NAME VARCHAR(256),
    CUSTOMER_LAST_NAME VARCHAR(256),
    CUSTOMER_ADDRESS VARCHAR(256),
    PRIMARY KEY (ID_SK)
);

-- Target TRANSACTION
create table if not exists TGT.DWH_D_CATEGORY_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    CATEGORY_DESC VARCHAR(1024),
    PRIMARY KEY (ID_SK)
);

-- Target TRANSACTION
create table if not exists TGT.DWH_D_SUBCATEGORY_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    CATEGORY_ID_SK NUMBER(38,0),
    SUBCATEGORY_DESC VARCHAR(256),
    PRIMARY KEY (ID_SK),
    FOREIGN KEY (CATEGORY_ID_SK) REFERENCES TGT.DWH_D_CATEGORY_LU(ID_SK)
);

-- Target PRODUCT
create table if not exists TGT.DWH_D_PRODUCT_LU (
    ID_SK INT AUTOINCREMENT,
    SOURCE_ID NUMBER(38,0) NOT NULL,
    SUBCATEGORY_ID_SK NUMBER(38,0),
    PRODUCT_DESC VARCHAR(256),
    PRIMARY KEY (ID_SK),
    FOREIGN KEY (SUBCATEGORY_ID_SK) REFERENCES TGT.DWH_D_SUBCATEGORY_LU(ID_SK)
);

-- Target SALES
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

