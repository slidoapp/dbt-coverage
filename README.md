# dbt-coverage

A CLI library with Python backend for computing coverage of dbt managed data
warehouses.

## Installation

```
pip install git+https://github.com/slidoapp/dbt-coverage.git
```

## Usage

`dbt-coverage` comes with two basic commands: `compute` and `compare`. The
documentation for the individual commands can be shown by using the `--help`
option.

### Compute

```
$ cd my-dbt-project
$ dbt docs generate  # Generate catalog.json and manifest.json
$ dbt-coverage compute  # Create coverage.json file and print coverage

# Coverage summary
# =======================================
# my-table-1               8/20     40.0%
# my-table-2              20/20    100.0%
# my-table-3               2/8      25.0%
# =======================================
# Total                   30/48     62.5%
```

### Compare

```
$ dbt-coverage compare coverage-after.json coverage-before.json

# Coverage delta summary
#               before     after            +/-
# =============================================
# Coverage      62.50%    60.00%         -2.50%
# =============================================
# Tables             3         4          +1/+0
# Columns           48        50          +2/+0
# =============================================
# Hits              30        30          +0/+0
# Misses            18        20          +2/+0
# =============================================

# New misses
# Catalog                  30/48   (62.50%)  ->    30/50   (60.00%) 
# ==================================================================
# - my-table-4              -/-       (-)    ->     0/2     (0.00%) 
# -- my-column-1            -/-       (-)    ->     0/1     (0.00%) 
# -- my-column-2            -/-       (-)    ->     0/1     (0.00%) 
# ==================================================================
```

### Combined use-case

```
$ cd my-dbt-project

$ dbt docs generate  # Generate catalog.json and manifest.json
$ dbt-coverage compute --cov-report before.json --cov-fail-under 0.5 # Fail if coverage is lower than 50%

# Make changes to the dbt project, e.g. add some columns to the DWH, document some columns, etc.

$ dbt docs generate
$ dbt-coverage compute --cov-report after.json --cov-fail-compare before.json  # Fail if the current coverage is lower than coverage in before.json
$ dbt-coverage compare after.json before.json  # Generate a detailed coverage delta report
```
