# Redshift SQL Syntax Guide

This guide provides information about the correct SQL syntax for Redshift, particularly for distribution and sort keys.

## Distribution Keys

In Redshift, distribution keys determine how data is distributed across the compute nodes in a cluster.

### Setting Distribution Keys

#### During Table Creation

```sql
CREATE TABLE my_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP
)
DISTKEY(id);
```

#### After Table Creation

```sql
ALTER TABLE my_table ALTER DISTKEY id;
```

Note: You cannot use `ALTER TABLE my_table ALTER DISTSTYLE KEY DISTKEY (id);` as this is not valid Redshift syntax.

## Sort Keys

Sort keys determine the order in which rows are stored in a table.

### Setting Sort Keys

#### During Table Creation

```sql
CREATE TABLE my_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP
)
SORTKEY(id, created_at);
```

#### After Table Creation

```sql
ALTER TABLE my_table ALTER SORTKEY (id, created_at);
```

Note: You cannot use `ALTER TABLE my_table ALTER COMPOUND SORTKEY (id, created_at);` as this is not valid Redshift syntax.

## Distribution Styles

Redshift supports several distribution styles:

### EVEN Distribution

Distributes data evenly across all nodes.

```sql
CREATE TABLE my_table (
    id INT PRIMARY KEY,
    name VARCHAR(100)
)
DISTSTYLE EVEN;
```

Or after creation:

```sql
ALTER TABLE my_table ALTER DISTSTYLE EVEN;
```

### KEY Distribution

Distributes data based on the values in the DISTKEY column.

```sql
CREATE TABLE my_table (
    id INT PRIMARY KEY,
    name VARCHAR(100)
)
DISTSTYLE KEY DISTKEY(id);
```

Or after creation:

```sql
ALTER TABLE my_table ALTER DISTKEY id;
```

### ALL Distribution

Distributes a copy of the entire table to every node.

```sql
CREATE TABLE my_table (
    id INT PRIMARY KEY,
    name VARCHAR(100)
)
DISTSTYLE ALL;
```

Or after creation:

```sql
ALTER TABLE my_table ALTER DISTSTYLE ALL;
```

## Common Errors

### Invalid Syntax for Distribution Keys

```
syntax error at or near "(" in context "DISTSTYLE KEY DISTKEY (", at line 67, column 56
LINE 67: ALTER TABLE raw_apartments ALTER DISTSTYLE KEY DISTKEY (id);
```

**Solution**: Use `ALTER TABLE raw_apartments ALTER DISTKEY id;` instead.

### Invalid Syntax for Sort Keys

```
syntax error at or near "COMPOUND" in context "ALTER TABLE raw_apartments ALTER COMPOUND", at line 68
LINE 68: ALTER TABLE raw_apartments ALTER COMPOUND SORTKEY (id, last_modified_timestamp);
```

**Solution**: Use `ALTER TABLE raw_apartments ALTER SORTKEY (id, last_modified_timestamp);` instead.

## References

- [Amazon Redshift Documentation - ALTER TABLE](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html)
- [Amazon Redshift Documentation - CREATE TABLE](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
- [Amazon Redshift Distribution Styles](https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html)
