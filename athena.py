import boto3

# Crear una sesión predeterminada de Boto3
session = boto3.Session(region_name='us-east-1')
athena_client = session.client('athena')

# Parámetros de la consulta
database_name = 'datawarehouse_sakila'
output_location = 's3://result-athena-1/'

# Consultas para crear las tablas
create_dim_customer_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `datawarehouse_sakila`.`dim_customer` (
  `customer_key` int,
  `customer_id` int,
  `first_name` string,
  `last_name` string,
  `email` string,
  `address` string,
  `city` string,
  `country` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://datosakilawarehouse/final/dim_customer/'
TBLPROPERTIES ('classification' = 'parquet');
"""

create_dim_film_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `datawarehouse_sakila`.`dim_film` (
  `film_key` int,
  `film_id` int,
  `title` string,
  `description` string,
  `release_year` int,
  `language` string,
  `category` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://datosakilawarehouse/final/dim_film/'
TBLPROPERTIES ('classification' = 'parquet');
"""

create_dim_date_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `datawarehouse_sakila`.`dim_date` (
  `date_key` int,
  `date` date,
  `day` int,
  `month` int,
  `year` int,
  `day_of_week` string,
  `is_holiday` boolean,
  `is_weekend` boolean,
  `quarter` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://datosakilawarehouse/final/dim_date/'
TBLPROPERTIES ('classification' = 'parquet');
"""

create_dim_category_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `datawarehouse_sakila`.`dim_category` (
  `category_key` int,
  `category_id` int,
  `name` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://datosakilawarehouse/final/dim_category/'
TBLPROPERTIES ('classification' = 'parquet');
"""

create_fact_rentals_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `datawarehouse_sakila`.`fact_rentals` (
  `rental_key` int,
  `rental_date` date,
  `return_date` date,
  `customer_key` int,
  `film_key` int,
  `date_key` int,
  `customer_name` string,
  `film_title` string

)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://datosakilawarehouse/final/fact_rentals/'
TBLPROPERTIES ('classification' = 'parquet');
"""

# Ejecutar la consulta en Athena


def run_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    return response


# Ejecutar la creación de las tablas
queries = [
    create_dim_customer_table_query,
    create_dim_film_table_query,
    create_dim_date_table_query,
    create_dim_category_table_query,
    create_fact_rentals_table_query
]

for query in queries:
    response = run_athena_query(query)
    print(
        "Table creation query submitted. Query execution ID:",
        response['QueryExecutionId'])