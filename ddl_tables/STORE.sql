create or replace TABLE STORE (
	ID NUMBER(38,0) NOT NULL,
	REGION_ID NUMBER(38,0),
	STORE_DESC VARCHAR(256),
	primary key (ID),
	foreign key (REGION_ID) references BHATBHATENI.TRANSACTIONS.REGION(ID)
);