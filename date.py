import boto3
import mysql.connector
import logging
import holidays
from datetime import datetime, timedelta


# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def lambda_handler(event, context):
    logger.info("Lambda function has started.")

    # Configuración de conexión a MySQL
    mysql_config = {
        'host': 'database-1.cjoy8kmcyakj.us-east-1.rds.amazonaws.com',
        'user': 'admin',
        'password': 'tres0916',
        'database': 'datawarehouse_sakila'  # Asegúrate de usar la base de datos correcta
    }

    # Conexión a MySQL
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        # Definir el rango de fechas para actualizar (puede ser mensual o
        # anual)
        today = datetime.today()
        start_date = today.replace(day=1)
        end_date = (start_date + timedelta(days=365)).replace(day=31)

        # Generar días festivos de Estados Unidos para el rango de fechas
        us_holidays = holidays.US(years=[today.year, today.year + 1])

        # Actualizar o insertar registros en la tabla dim_date
        current_date = start_date
        while current_date <= end_date:
            is_holiday = current_date in us_holidays
            cursor.execute("""
                INSERT INTO dim_date (date, is_holiday)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                is_holiday = VALUES(is_holiday)
            """, (current_date, is_holiday))
            current_date += timedelta(days=1)

        conn.commit()
        logger.info(f"Date dimension updated from {start_date} to {end_date}.")

    except mysql.connector.Error as err:
        logger.error(f"MySQL Error: {err}")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': 'Date dimension updated successfully'
    }