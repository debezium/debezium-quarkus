-- Initialize Oracle database for testing
CONNECT native/dbz@FREEPDB1;

-- Setup USERS
CREATE TABLE native.users (id NUMBER(9,0) PRIMARY KEY, name varchar2(255) NOT NULL);
ALTER TABLE native.users ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON native.users to c##dbzuser;

INSERT INTO native.users (id,name) values (1, 'alvar');
INSERT INTO native.users (id,name) values (2, 'anisha');
INSERT INTO native.users (id,name) values (3, 'chris');
INSERT INTO native.users (id,name) values (4, 'indra');
INSERT INTO native.users (id,name) values (5, 'jiri');
INSERT INTO native.users (id,name) values (6, 'giovanni');
INSERT INTO native.users (id,name) values (7, 'mario');
INSERT INTO native.users (id,name) values (8, 'rené');
INSERT INTO native.users (id,name) values (9, 'Vojtech');

-- Setup PRODUCTS
CREATE TABLE native.products (id NUMBER(9,0) PRIMARY KEY, name varchar2(255) NOT NULL, description varchar2(255) NOT NULL);
ALTER TABLE native.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON native.products to c##dbzuser;

INSERT INTO native.products (id,name,description) values (1,'t-shirt', 'red hat t-shirt');
INSERT INTO native.products (id,name,description) values (2, 'sweatshirt', 'blue ibm sweatshirt');

COMMIT;