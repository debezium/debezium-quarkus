CREATE SCHEMA IF NOT EXISTS alternative;

CREATE TABLE alternative.orders(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL
);

INSERT INTO alternative.orders(id, name, description)
VALUES (1, 'pizza', 'pizza with peperoni'),
       (2, 'kebab', 'kebab with mayonnaise')