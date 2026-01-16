CREATE SCHEMA IF NOT EXISTS native;

CREATE TABLE native.products(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL
);

CREATE TABLE native.users(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO native.users (name) VALUES ('alvar'), ('anisha'), ('chris'), ('indra'), ('jiri'), ('giovanni'), ('mario'), ('rené'), ('Vojtech');
INSERT INTO native.products (id, name, description) VALUES (1, 't-shirt','red hat t-shirt'), (2,'sweatshirt','blue ibm sweatshirt');
