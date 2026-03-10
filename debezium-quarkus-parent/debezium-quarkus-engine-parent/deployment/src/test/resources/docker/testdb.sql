/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

set lock mode to wait;
create database testdb with log;

database testdb;

CREATE TABLE products (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight DECIMAL(10,2)
);

INSERT INTO products VALUES (1, 'Product 1', 'Description 1', 10.5);
INSERT INTO products VALUES (2, 'Product 2', 'Description 2', 20.3);
INSERT INTO products VALUES (3, 'Product 3', 'Description 3', 30.1);
