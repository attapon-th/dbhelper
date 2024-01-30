# Database Helper

### `Version: 0.7.x`

[![GitHub version](https://badge.fury.io/gh/attapon-th%2Fdbhelper.svg)](https://badge.fury.io/gh/attapon-th%2Fdbhelper)
[![GitHub release version](https://img.shields.io/github/v/release/attapon-th/dbhelper?include_prereleases)](https://github.com/attapon-th/dbhelper)


- [Database Helper](#database-helper)
    - [`Version: 0.7.x`](#version-07x)
  - [Getting Started](#getting-started)
    - [Use:](#use)
    - [Install SQLAlchemy Database API](#install-sqlalchemy-database-api)
  - [Process SQL file](#process-sql-file)
    - [Usage](#usage)
  - [Dump SQL Query to CSV file](#dump-sql-query-to-csv-file)
    - [Usage](#usage-1)
  - [Dump SQL Query to Parquet file](#dump-sql-query-to-parquet-file)



## Getting Started

```bash
pip install git+https://github.com/attapon-th/dbhelper@latest
```

### Use:

```base
dbper --help
```

### Install SQLAlchemy Database API

```bash
# mysql
# dsn=mysql+pymysql://user:pass@host:port/dbname
pip install pymysql

# postgres
# dsn=postgresql+psycopg2://user:pass@host:port/dbname
pip install psycopg2-binary

# vertica
# dsn=vertica+vertica_python://user:pass@host:port/dbname
pip install git+https://github.com/attapon-th/sqlalchemy-vertica-python.git@latest

# other sqlalchemy support
```
> other sqlalchemy support
> [https://docs.sqlalchemy.org/en/20/core/engines.html](https://docs.sqlalchemy.org/en/20/core/engines.html)

## Process SQL file

### Usage 

```bash
export DB_DSN=vertica+vertica_python://user:pass@host:port/dbname
dbper process test.sql 

# or 

dbper process --dsn "vertica+vertica_python://user:pass@host:port/dbname" test.sql
```

example `test.sql`****
```sql
-- test.sql
-- create table
CREATE TABLE IF NOT EXISTS test (
  id int,
  name text
);

-- insert data
INSERT INTO test (id, name) VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c');

-- select data
SELECT * FROM test;
```


---


## Dump SQL Query to CSV file

### Usage
```bash
export DB_DSN=vertica+vertica_python://user:pass@host:port/dbname

dbper csv \
  --output test.csv
  "SELECT * FROM test"

# or
dbper csv \
  --dsn "vertica+vertica_python://user:pass@host:port/dbname" \
  --output test.csv
  "SELECT * FROM test"

```

## Dump SQL Query to Parquet file

```bash
export DB_DSN=vertica+vertica_python://user:pass@host:port/dbname
dbper parquet \
  --output test.parquet \
  "SELECT * FROM test" 


# or
dbper parquet \
  --dsn "vertica+vertica_python://user:pass@host:port/dbname" \
  --output test.parquet \
  "SELECT * FROM test" 
```