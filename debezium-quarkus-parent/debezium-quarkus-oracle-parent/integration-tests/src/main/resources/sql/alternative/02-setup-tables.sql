-- Initialize Oracle database for testing
CONNECT alternative/dbz@FREEPDB1;

-- Setup USERS
CREATE TABLE alternative.users (id NUMBER(9,0) PRIMARY KEY, name varchar2(255) NOT NULL);
ALTER TABLE alternative.users ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON alternative.users to c##dbzuser;

INSERT INTO alternative.users (id,name) values (1, 'alvar');
INSERT INTO alternative.users (id,name) values (2, 'anisha');
INSERT INTO alternative.users (id,name) values (3, 'chris');
INSERT INTO alternative.users (id,name) values (4, 'indra');
INSERT INTO alternative.users (id,name) values (5, 'jiri');
INSERT INTO alternative.users (id,name) values (6, 'giovanni');
INSERT INTO alternative.users (id,name) values (7, 'mario');
INSERT INTO alternative.users (id,name) values (8, 'rené');
INSERT INTO alternative.users (id,name) values (9, 'Vojtech');

-- Setup PRODUCTS
CREATE TABLE alternative.products (id NUMBER(9,0) PRIMARY KEY, name varchar2(255) NOT NULL, description varchar2(255) NOT NULL);
ALTER TABLE alternative.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON alternative.products to c##dbzuser;

INSERT INTO alternative.products (id,name,description) values (1,'t-shirt', 'red hat t-shirt');
INSERT INTO alternative.products (id,name,description) values (2, 'sweatshirt', 'blue ibm sweatshirt');

COMMIT;