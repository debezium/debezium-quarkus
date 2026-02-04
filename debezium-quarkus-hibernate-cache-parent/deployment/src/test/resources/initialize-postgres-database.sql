CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.PRODUCT(custom_id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);
CREATE TABLE inventory.Order(id BIGINT NOT NULL PRIMARY KEY, description VARCHAR(255) NOT NULL);
CREATE TABLE inventory.User(id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);
CREATE TABLE inventory.Fruit(id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);

INSERT INTO inventory.Order (id, description) VALUES (1,'one'), (2,'two');
INSERT INTO inventory.User (id, name) VALUES (1,'giovanni'), (2,'mario');
INSERT INTO inventory.Fruit (id, name) VALUES (1,'apple'), (2,'banana');
INSERT INTO inventory.PRODUCT (custom_id, name) VALUES (1,'t-shirt'), (2,'thinkpad');