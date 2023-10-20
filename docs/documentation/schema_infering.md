<!--
# To improve the naming of the datalake and avoid refactor the project, move the basic datalake temporally
$ mv tests/data/datalake_with_config_schema/bar tests/data/datalake_with_config_schema/school
$ mv tests/data/datalake_with_config_schema/foo tests/data/datalake_with_config_schema/grades
$ mv tests/data/basic_datalake/bar tests/data/basic_datalake/school
$ mv tests/data/basic_datalake/foo tests/data/basic_datalake/grades
$ echo "school.courses:
>   id: int
>   course_name: string
> school.students:
>   id: int
>   first_name: string
>   last_name: string
>   email: string
>   gender: string
>   birth_date: date
> " > tests/data/datalake_with_config_schema/schema_config.yaml
-->
# Schema inferring
`pyspark-data-mocker` lets you define the schema of the table as you please. You can enable automatic schema inferring
by setting up the `schema.infer` option in the [configuration file](https://fedemgp.github.io/documentation/configuration#configuration-file-explanation),
or you can manually specify the schema of each column you want using another yaml file. By default,
`pyspark-data-mocker` will consider that all columns are `string` columns

## Automatic inferring
This is the simplest configuration. Let's see the example we saw before in the [welcome page](https://fedemgp.github.io)
with automatic infer schema enabled

```bash
$ cat ./tests/data/config/infer_schema.yaml
spark_configuration:
  app_name: "test"
  number_of_cores: 1
schema:
  infer: True
```

You only need to set the boolean `schema.infer` to `True` and that is it! once you load the builder, the columns
will vary depending on their values

```python
>>> from pyspark_data_mocker import DataLakeBuilder
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/basic_datalake", "./tests/data/config/infer_schema.yaml")  # byexample: +timeout=20 +pass
>>> spark = builder.spark
>>> spark.sql("DESCRIBE TABLE school.students").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|      int|
|first_name|   string|
| last_name|   string|
|     email|   string|
|    gender|   string|
|birth_date|   string|
+----------+---------+


>>> spark.sql("DESCRIBE TABLE school.courses").select("col_name", "data_type").show()
+-----------+---------+
|   col_name|data_type|
+-----------+---------+
|         id|      int|
|course_name|   string|
+-----------+---------+


>>> spark.sql("DESCRIBE TABLE grades.exams").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|      int|
|student_id|      int|
| course_id|      int|
|      date|   string|
|      note|      int|
+----------+---------+

>>> builder.cleanup()
```

## Schema configuration file
This yaml file needs to be located in the folder you will place the datalake definition (the root path you will
pass to the `DatalakeBuilder` class). By default, the config file that will be used is called `schema_config.yaml`,
but it can be overriden in the [application configuration file](https://fedemgp.github.io/documentation/configuration#configuration-file-explanation).

```bash
$ cat ./pyspark_data_mocker/config/schema.py
<...>schema.Optional("config_file", default="schema_config.yaml")<...>
```

That yaml needs to be a file where each key represents the table name (considering the database), and as value,
a dictionary with the columns as keys, and a [Spark's DDL type of the column](https://vincent.doba.fr/posts/20211004_spark_data_description_language_for_defining_spark_schema/) as value.

## Example
Let's consider this datalake definition.

```bash
$ tree tests/data/datalake_with_config_schema -n --charset=ascii  # byexample: +rm=~ +skip
tests/data/datalake_with_config_schema
|-- grades
|   `-- exams.csv
|-- schema_config.yaml
`-- school
    |-- courses.csv
    `-- students.csv
~
2 directories, 4 files
```

Notice how in this example, unlike the one seen previously in the [Home section](https://fedemgp.github.io) contains a
file `schema_config.yaml`. The content of this file will define the types of each column of the tables.

```bash
$ cat tests/data/datalake_with_config_schema/schema_config.yaml
school.courses:
  id: int
  course_name: string
school.students:
  id: int
  first_name: string
  last_name: string
  email: string
  gender: string
  birth_date: date
```

Take a moment to digest the schema of the file. How each key of the yaml dictionary is the full name of the table
that will be created, and as value contains another dictionary with the columns of the table, and the type of that column.
Let's build up the datalake

```python
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/datalake_with_config_schema")  # byexample: +timeout=20 +pass
>>> spark = builder.spark

>>> spark.sql("SHOW TABLES IN school").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|   school|  courses|      false|
|   school| students|      false|
+---------+---------+-----------+


>>> spark.sql("SHOW TABLES IN grades").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|   grades|    exams|      false|
+---------+---------+-----------+
```

Now the tables are loaded, we can take a look at the schema of each table.

```python
>>> spark.sql("DESCRIBE TABLE school.students").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|      int|
|first_name|   string|
| last_name|   string|
|     email|   string|
|    gender|   string|
|birth_date|     date|
+----------+---------+


>>> spark.sql("DESCRIBE TABLE school.courses").select("col_name", "data_type").show()
+-----------+---------+
|   col_name|data_type|
+-----------+---------+
|         id|      int|
|course_name|   string|
+-----------+---------+


>>> spark.sql("DESCRIBE TABLE grades.exams").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|   string|
|student_id|   string|
| course_id|   string|
|      date|   string|
|      note|   string|
+----------+---------+

>>> builder.cleanup()
```

<!--
>>> spark.sql("SHOW DATABASES").show()
+---------+
|namespace|
+---------+
|  default|
+---------+
-->

Now the column types changed! we have the `birth_date` that is `date` type and the ids as `int`. Notice also that
the table `grades.exams` (which we didn't define any custom schema) has for each column the default value `string`
(because it's the fallback type as we saw before).

## Combining both schema inferring configurations
We can combine this file with the automatic `infer` option to only configure manually the schemas that we need.

```python
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/datalake_with_config_schema", "./tests/data/config/infer_schema.yaml")  # byexample: +timeout=20 +pass
>>> spark = builder.spark
>>> spark.sql("DESCRIBE TABLE school.students").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|      int|
|first_name|   string|
| last_name|   string|
|     email|   string|
|    gender|   string|
|birth_date|     date|
+----------+---------+


>>> spark.sql("DESCRIBE TABLE school.courses").select("col_name", "data_type").show()
+-----------+---------+
|   col_name|data_type|
+-----------+---------+
|         id|      int|
|course_name|   string|
+-----------+---------+


>>> spark.sql("DESCRIBE TABLE grades.exams").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|      int|
|student_id|      int|
| course_id|      int|
|      date|   string|
|      note|      int|
+----------+---------+

>>> builder.cleanup()
```
Now the `grades.exams` table schema also changed! but take into consideration that the automatic schema inferring of spark
**it's not magic**. Note that the date column of `grades.exams` was not inferred to a `date` column type.
Sometimes it is needed to use the manual schema definition to have the value we need.


> **NOTE**: This behavior is fixed starting from pyspark 3.3. From that version and beyond, it infers date columns, but
> spark considers all date-kind values as datetime
## Column types
You can define the type of column of each type that [Spark supports](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)!
you don't have any restriction whatsoever (kind of, but more of that later). 

### Example
Consider these files and schema definitions
```bash
$ tree tests/data/datalake_different_files_and_schemas -n --charset=ascii  # byexample: +rm=~ +skip
tests/data/datalake_different_files_and_schemas
|-- schema_config.yaml
`-- school
    |-- courses.json
    `-- students.csv
~
1 directory, 3 files

$ cat tests/data/datalake_different_files_and_schemas/school/courses.json
{"id": 1, "course_name": "Algorithms 1", "flags": {"acitve": true}, "correlative_courses": []}
{"id": 2, "course_name": "Algorithms 2", "flags": {"acitve": true}, "correlative_courses": [1]}

$ cat tests/data/datalake_different_files_and_schemas/schema_config.yaml
school.courses:
  id: int
  course_name: string
  flags: map<string, boolean>
  correlative_courses: array<int>
school.students:
  id: long
  name: string
  birth_date: date
```

`pyspark-data-mocker` does not need that all files are in the same file format, it will infer each file depending on
the file extension (the limitation is that the file format is a valid spark source). Let's see the schemas and data
in each table.

```python
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/datalake_different_files_and_schemas")  # byexample: +timeout=20 +pass
>>> spark = builder.spark
>>> spark.sql("DESCRIBE TABLE school.students").select("col_name", "data_type").show()
+----------+---------+
|  col_name|data_type|
+----------+---------+
|        id|   bigint|
|      name|   string|
|birth_date|     date|
+----------+---------+

>>> spark.table("school.students").show()
+---+----------------+----------+
| id|            name|birth_date|
+---+----------------+----------+
|  1|Shirleen Dunford|1978-08-01|
|  2|    Niko Puckrin|2000-11-28|
|  3|   Sergei Barukh|1992-01-20|
|  4|     Sal Maidens|2003-12-14|
|  5|Cooper MacGuffie|2000-03-07|
+---+----------------+----------+



>>> spark.sql("DESCRIBE TABLE school.courses").select("col_name", "data_type").show()
+-------------------+-------------------+
|           col_name|          data_type|
+-------------------+-------------------+
|                 id|                int|
|        course_name|             string|
|              flags|map<string,boolean>|
|correlative_courses|         array<int>|
+-------------------+-------------------+

>>> spark.table("school.courses").show()
+---+------------+----------------+-------------------+
| id| course_name|           flags|correlative_courses|
+---+------------+----------------+-------------------+
|  1|Algorithms 1|{acitve -> true}|                 []|
|  2|Algorithms 2|{acitve -> true}|                [1]|
+---+------------+----------------+-------------------+


>>> builder.cleanup()
```


### Limitations
The limitation you have is that you cannot define certain column types in some file formats.
An example of that is to define a `map` column type in a `csv` file.

```bash
$ cat tests/data/column_type_not_supported/schema_config.yaml
foo.bar:
  id: int
  invalid_col: map<string, string>

$ tree tests/data/column_type_not_supported -n --charset=ascii  # byexample: +rm=~ +skip
tests/data/column_type_not_supported
|-- foo
|   `-- bar.csv
`-- schema_config.yaml
~
1 directory, 2 files
```

If we want to set up this datalake, it will fail with this exception message.

```python
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/column_type_not_supported")  # byexample: +timeout=20
<...>AnalysisException: CSV data source does not support map<string,string> data type.
```

<!--
$ mv tests/data/datalake_with_config_schema/school tests/data/datalake_with_config_schema/bar
$ mv tests/data/datalake_with_config_schema/grades tests/data/datalake_with_config_schema/foo
$ mv tests/data/basic_datalake/school tests/data/basic_datalake/bar
$ mv tests/data/basic_datalake/grades tests/data/basic_datalake/foo
$ echo "bar.courses:
>   id: int
>   course_name: string
> bar.students:
>   id: int
>   first_name: string
>   last_name: string
>   email: string
>   gender: string
>   birth_date: date" > tests/data/datalake_with_config_schema/schema_config.yaml
-->