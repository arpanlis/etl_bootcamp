create or replace TABLE REGION (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_ID NUMBER(38,0),
	REGION_DESC VARCHAR(256),
	primary key (ID),
	foreign key (COUNTRY_ID) references BHATBHATENI.TRANSACTIONS.COUNTRY(ID)
);