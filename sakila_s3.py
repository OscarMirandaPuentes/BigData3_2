import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3
import mysql.connector
import pandas as pd
import logging
from io import StringIO
import csv

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Inicializar Spark y Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def upload_to_s3(bucket_name, s3_prefix, df, filename):
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar='\\')
    s3_key = f"{s3_prefix}{filename}"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer.getvalue())
    logger.info(f"Data uploaded to S3 at {s3_key}")


def main():
    logger.info("Job started.")

    # Configuración de conexión a MySQL
    mysql_config = {
        'host': 'database-1.cjoy8kmcyakj.us-east-1.rds.amazonaws.com',
        'user': 'admin',
        'password': 'tres0916',
        'database': 'sakila'  # Asegúrate de usar la base de datos correcta
    }

    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        # Definir las tablas que queremos copiar
        tables = [
            'actor',
            'address',
            'category',
            'city',
            'country',
            'customer',
            'film',
            'film_actor',
            'film_category',
            'inventory',
            'language',
            'payment',
            'rental',
            'staff',
            'store']

        # Configuración de AWS S3
        bucket_name = 'sakila-warehouse-1'
        s3_prefix = 'landing/'

        for table in tables:
            try:
                logger.info(f"Copying table {table}")
                cursor.execute(f"SELECT * FROM {table}")
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                if data:
                    logger.info(f"Fetched {len(data)} rows from table {table}")
                    df = pd.DataFrame(data, columns=columns)
                    upload_to_s3(bucket_name, s3_prefix, df, f"{table}.csv")
                else:
                    logger.warning(f"No data found in table {table}")
            except Exception as e:
                logger.error(f"Error processing table {table}: {e}")

    except mysql.connector.Error as err:
        logger.error(f"MySQL Error: {err}")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    logger.info("Job finished successfully.")


if __name__ == "__main__":
    main()

job.commit()