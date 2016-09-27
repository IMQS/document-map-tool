# DocumentGeometry translation tool

The document geometry tool reads document records from MongoDB, translates the data into valid SQL and inserts the result into Postgres.

## USAGE:
```
docgeom -c=configfile [options] command

[options]
  c: Configuration file path. This option is mandatory

[commands]
  run: Run the docgeom tool
```
