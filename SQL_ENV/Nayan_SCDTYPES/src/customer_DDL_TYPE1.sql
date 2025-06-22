
create schema webshop;


--raw_table-----

CREATE TABLE webshop.raw_customer (
    id integer NOT NULL,
    firstname text,
    lastname text,
    gender text,
    email text,
    dateofbirth date,
    currentaddressid integer
);



---dim-table

CREATE TABLE webshop.dim_customer (
	id int4 NOT NULL,
	firstname text NULL,
	lastname text NULL,
	gender text NULL,
	email text NULL,
	dateofbirth date NULL,
	currentaddressid int4 NULL,
	ins_dt varchar NULL,
	upd_dt varchar NULL
);

--type dim table
CREATE TABLE webshop.dim_customer_type2 (
    id INT NOT NULL,                
    firstname TEXT,
    lastname TEXT,
    gender TEXT,
    email TEXT,
    dateofbirth DATE,
    currentaddressid INT,
    is_current BOOLEAN DEFAULT TRUE,       
    start_date DATE DEFAULT CURRENT_DATE,   
    end_date DATE DEFAULT NULL             
);
