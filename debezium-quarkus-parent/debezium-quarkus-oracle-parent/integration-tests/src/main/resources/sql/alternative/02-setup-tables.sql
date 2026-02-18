-- Initialize Oracle database for testing
CONNECT alternative/dbz@FREEPDB1;

-- Setup USERS
CREATE TABLE alternative.orders (id NUMBER(9,0) PRIMARY KEY, name varchar2(255) NOT NULL, description varchar2(255) NOT NULL);
ALTER TABLE alternative.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON alternative.orders to c##dbzuser;

INSERT INTO alternative.orders (id,name,description) values (1, 'pizza', 'pizza with peperoni');
INSERT INTO alternative.orders (id,name,description) values (2, 'kebab', 'kebab with mayonnaise');

COMMIT;