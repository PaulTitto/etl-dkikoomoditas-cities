CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT stg AUTHORIZATION postgres;

CREATE TABLE stg.commodity_source (
    id uuid default uuid_generate_v4(),
    value integer,
    "time" text,
    commodity_id integer,
    commodity_name text,
    avg_value integer,
    max_value integer,
    min_value integer,
    city_id integer,
    city_name text,
    "Year" integer,
    "Month" integer
);
