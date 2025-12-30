CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.products(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);
CREATE TABLE inventory.general_table(id INT NOT NULL PRIMARY KEY);
CREATE TABLE inventory.orders(id INT NOT NULL PRIMARY KEY, "key" INT NOT NULL, name VARCHAR(255) NOT NULL);
CREATE TABLE inventory.users(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL);

INSERT INTO inventory.general_table(id) VALUES (1);
INSERT INTO inventory.orders (id, "key", name) VALUES (1,1, 'one'), (2,2,'two');
INSERT INTO inventory.users (id, name, description) VALUES (1,'giovanni', 'developer'), (2,'mario', 'developer');
INSERT INTO inventory.products (id, name) VALUES (1,'t-shirt'), (2,'thinkpad');