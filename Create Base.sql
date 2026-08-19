CREATE CATALOG IF NOT EXISTS claims_lakehouse;

CREATE SCHEMA IF NOT EXISTS claims_lakehouse.raw;

CREATE SCHEMA IF NOT EXISTS claims_lakehouse.bronze;

CREATE SCHEMA IF NOT EXISTS claims_lakehouse.silver;

CREATE SCHEMA IF NOT EXISTS claims_lakehouse.gold;

CREATE SCHEMA IF NOT EXISTS claims_lakehouse.governance;

CREATE VOLUME IF NOT EXISTS
claims_lakehouse.raw.synthea_ingress;