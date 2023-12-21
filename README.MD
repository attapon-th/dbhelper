# Database Helper

### `Version: 0.7.x`

[![GitHub version](https://badge.fury.io/gh/attapon-th%2Fdbhelper.svg)](https://badge.fury.io/gh/attapon-th%2Fdbhelper)
[![GitHub release version](https://img.shields.io/github/v/release/attapon-th/dbhelper?include_prereleases)](https://github.com/attapon-th/dbhelper)


- [Database Helper](#database-helper)
    - [`Version: 0.7.x`](#version-07x)
  - [Getting Started](#getting-started)
    - [Use:](#use)
    - [Install SQLAlchemy API](#install-sqlalchemy-api)
  - [Process SQL file](#process-sql-file)
    - [Usage](#usage)
    - [Help command](#help-command)
  - [Dump SQL Query to CSV file](#dump-sql-query-to-csv-file)
    - [Usage](#usage-1)
    - [Help command](#help-command-1)
  - [Dump SQL Query to Parquet file](#dump-sql-query-to-parquet-file)
    - [Help command](#help-command-2)



## Getting Started

```bash
pip install git+https://github.com/attapon-th/dbhelper@main
```

### Use:

```base
dbper --help
```

### Install SQLAlchemy API

```bash
# mysql
# dsn=mysql+pymysql://user:pass@host:port/dbname
pip install pymysql

# postgres
# dsn=postgresql+psycopg2://user:pass@host:port/dbname
pip install psycopg2-binary

# vertica
# dsn=vertica+vertica_python://user:pass@host:port/dbname
pip install sqlalchemy-vertica-python
# or 
pip install git+https://github.com/attapon-th/sqlalchemy-vertica-python.git

# other sqlalchemy support
```
> other sqlalchemy support
> [https://docs.sqlalchemy.org/en/20/core/engines.html](https://docs.sqlalchemy.org/en/20/core/engines.html)

## Process SQL file

### Usage 

```bash
DSN=vertica+vertica_python://user:pass@host:port/dbname
dbper process --dsn $DSN  test.sql 
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

### Help command
```bash
dbper process --help
```
```
Usage: dbper process [OPTIONS] FILE

  Process sql file

Options:
  -d, --dsn TEXT  SQLAlchemy connection string  [required]
  --help          Show this message and exit.
```

---


## Dump SQL Query to CSV file

### Usage
```bash
DSN=vertica+vertica_python://user:pass@host:port/dbname

dbper csv \
  --dsn $DSN \
  --sql "SELECT * FROM test" \
  test.csv # output file name
```

### Help command

```bash
dbper csv --help
```
```
Usage: dbper csv [OPTIONS] OUTPUT

  Dump SQL Statement to CSV file

  OUTPUT: `./test.csv.gz` is compressed with GZIP

  OR

  OUTPUT: `./test.csv` is not compressed

Options:
  -d, --dsn TEXT  Database connection string  [required]
  -s, --sql TEXT  SQL Query Statement[file|str]  [required]
  --help          Show this message and exit.
```
---

## Dump SQL Query to Parquet file

```bash
DSN=vertica+vertica_python://user:pass@host:port/dbname
dbper parquet \
  --dsn $DSN \
  --sql "SELECT * FROM test" \
  test.parquet # output file name
```

### Help command

```bash
dbper parquet --help
```
```
Usage: dbper parquet [OPTIONS] OUTPUT

  Dump SQL Statement to Parquet file

Options:
  -d, --dsn TEXT                  Database connection string  [required]
  -s, --sql TEXT                  SQL Query Statement[file|str]  [required]
  -c, --compression [snappy|gzip|brotli]
                                  Compression type
  --help                          Show this message and exit.
```