-- Initialize Oracle database for testing

-- Setup USERS
CREATE TABLE inventory.users (id NUMBER(9,0) PRIMARY KEY, name VARCHAR2(255) NOT NULL, description VARCHAR2(255) NOT NULL);
ALTER TABLE inventory.users ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON inventory.users TO c##dbzuser;

INSERT INTO inventory.users (id,name,description) VALUES (1, 'giovanni', 'developer');
INSERT INTO inventory.users (id,name,description) VALUES (2, 'mario', 'developer');

-- Setup PRODUCTS
CREATE TABLE inventory.products (id NUMBER(9,0) PRIMARY KEY, name VARCHAR2(255) NOT NULL, description VARCHAR2(255) NOT NULL);
ALTER TABLE inventory.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON inventory.products TO c##dbzuser;

INSERT INTO inventory.products (id,name,description) values (1, 't-shirt', 'red hat t-shirt');
INSERT INTO inventory.products (id,name,description) values (2, 'sweatshirt', 'blue ibm sweatshirt');

-- Setup ORDERS
CREATE TABLE inventory.orders (id NUMBER(9,0) PRIMARY KEY, key NUMBER(9,0) NOT NULL, name VARCHAR2(255) NOT NULL);
ALTER TABLE inventory.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT ON inventory.orders TO c##dbzuser;

INSERT INTO inventory.orders (id,key,name) values (1, 1, 'one');
INSERT INTO inventory.orders (id,key,name) values (2, 2, 'two');

COMMIT;