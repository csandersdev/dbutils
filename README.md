# dbutils
Command-line JDBC client that will execute SQL from script files and output the results in CSV format. The client comes with some useful features around substituting variables into scripts, using variable statement delimiters, and handling LOB type columns in the output. This should work with any JDBC driver, but some profiles are provided that will link in drivers for common RDBMS vendors that you might want to interact with. If your preferred vendor isn't listed, you can update env.cfg in the deployment so that the classpath includes the JDBC driver for the target system.

```
$> ./execsql.sh
Usage: execSQL [--quiet] [--noheader] [--munge] [--properties a=b,c=d,...] [--propFile <filename>] [--delimeter <sqldelim>] [--fieldSep <fieldSep>] [--datadir <dir>] [--lobColumnNames lobcolumn=filenamecolumn,...] [--overwrite] [--dbConfig <configFilePath>] <sqlfile> ...
```

Multiple SQL files can be executed with a single command. Each SQL file can contain multiple SQL statements. Statements are separated using the semicolon character or whatever is specified by the --delimiter flag. The target database is specified using the properties collection stored in the path pointed to by --dbConfig. If dbConfig is not specified, it is assumed to be db_config.properties in the current working directory.

Reports are rendered as CSV with column names included in the output. If you want to exclude column names, use the --noheader flag. The field separator can be changed to a different value (e.g. tab) using the --fieldSep flag.

If the report contains any LOB columns, the data will be stored into separate files from the report and the column in the report will be the filename of the file that is created. By default the system will generate unique filenames based on the column name in the result set. You can map those columns to specific file using the --lobColumnNames flag where the value of the flag is a comma-separated list of columname=filename pairs. By default filenames are assumed to be relative to the current working directly, but you may change the location using the --datadir flag. If a filename with the specified name already exists in the target location, the system will not overwrite unless the --overwrite flag is specified.

Simple entity substitution on the input files can be done on the fly using the --munge and --properties flags. Entity boundaries are started with and ampersand (&amp;) and end with a semicolon (;). For each entity you want to subsitute, provider an ENTITY_NAME=ENTITY_VALUE property. Properties on the command-line are separated by comma. Alternatively, you can pass the properties in Java properties file format using the --propFile argument. 

