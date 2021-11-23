# dbt-coverage

A CLI library with Python backend for computing docs and test coverage of dbt
managed data warehouses.

## Installation

```
pip install dbt-coverage
```

## Usage

`dbt-coverage` comes with two basic commands: `compute` and `compare`. The
documentation for the individual commands can be shown by using the `--help`
option.

### Compute

Compute coverage from `target/catalog.json` and `target/manifest.json` files
found in a dbt project, e.g.
[jaffle_shop](https://github.com/dbt-labs/jaffle_shop). You need to select
documentation or test coverage by using the respective CLI argument.

```
$ cd jaffle_shop
$ dbt run  # Materialize models
$ dbt docs generate  # Generate catalog.json and manifest.json
$ dbt-coverage compute doc --cov-report coverage-doc.json  # Compute doc coverage, print it and write it to coverage-doc.json file

Coverage report
=====================================================================
customers                                              6/7      85.7%
orders                                                 9/9     100.0%
raw_customers                                          0/3       0.0%
raw_orders                                             0/4       0.0%
raw_payments                                           0/4       0.0%
stg_customers                                          0/3       0.0%
stg_orders                                             0/4       0.0%
stg_payments                                           0/4       0.0%
=====================================================================
Total                                                 15/38     39.5%

$ dbt-coverage compute test --cov-report coverage-test.json  # Compute test coverage, print it and write it to coverage-test.json file

Coverage report
=====================================================================
customers                                              1/7      14.3%
orders                                                 8/9      88.9%
raw_customers                                          0/3       0.0%
raw_orders                                             0/4       0.0%
raw_payments                                           0/4       0.0%
stg_customers                                          1/3      33.3%
stg_orders                                             2/4      50.0%
stg_payments                                           2/4      50.0%
=====================================================================
Total                                                 14/38     36.8%
```

### Compare

Compare two `coverage.json` files generated by the `compute` command. This is
useful to ensure that the coverage does not drop while making changes to the
project.

```
$ dbt-coverage compare coverage-after.json coverage-before.json

# Coverage delta summary
              before     after            +/-
=============================================
Coverage      39.47%    38.46%         -1.01%
=============================================
Tables             8         8          +0/+0
Columns           38        39          +1/+0
=============================================
Hits              15        15          +0/+0
Misses            23        24          +1/+0
=============================================

# New misses
==============================================================
Catalog              15/38   (39.47%)  ->    15/39   (38.46%) 
==============================================================
- customers           6/7    (85.71%)  ->     6/8    (75.00%) 
-- new_col            -/-       (-)    ->     0/1     (0.00%) 
==============================================================
```

### Combined use-case

```
$ cd my-dbt-project

$ dbt run  # Materialize models
$ dbt docs generate  # Generate catalog.json and manifest.json
$ dbt-coverage compute doc --cov-report before.json --cov-fail-under 0.5  # Fail if coverage is lower than 50%

# Make changes to the dbt project, e.g. add some columns to the DWH, document some columns, etc.

$ dbt docs generate
$ dbt-coverage compute doc --cov-report after.json --cov-fail-compare before.json  # Fail if the current coverage is lower than coverage in before.json
$ dbt-coverage compare after.json before.json  # Generate a detailed coverage delta report
```

## Related packaged

https://github.com/mikaelene/dbt-test-coverage

## License

Licensed under the MIT license (see [LICENSE.md](LICENSE.md) file for more
details).