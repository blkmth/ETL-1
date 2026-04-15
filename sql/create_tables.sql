-- suppression si existantes
DROP TABLES IF EXISTS sales_clean ;

     --table cible du pipeline
CREATE TABLE sales_clean (
    id              SERIAL PRIMARY KEY,
    transactions_id varchar(50)    not null unique,
    customer_name   varchar(50)    not null,
    product         varchar(50)    not null,
    category        varchar(50)    not null,
    qauntity        integer        not null check (qauntity > 0),
    unit_price      numeric(10, 2) not null check (unit_price > 0),
    total_amount    numeric(10, 2) not null check (total_amount > 0),
    sale_date       date           not null,
    country         varchar(50)    not null,
    loaded_at       timestamp default now()
) ;

-- Index pour accelerer les requetes analytiques
CREATE INDEX idx_sale_date ON sales_clean (sale_date) ;
CREATE INDEX idx_country ON sales_clean (country) ;
CREATE INDEX idx_category ON sales_clean (category) ;

COMMENT ON TABLE sales_clean IS 'Table cible du pipeline' ;

