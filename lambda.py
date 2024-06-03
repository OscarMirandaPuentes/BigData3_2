import boto3
import mysql.connector
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import random
from io import StringIO
# from pyspark.sql import SparkSession

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
        'database': 'sakila'  # Asegúrate de usar la base de datos correcta
    }

    # Crear sesión Spark
    # spark = SparkSession.builder.appName("GlueJobSimulation").getOrCreate()

    # Conexión a MySQL
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        # Obtener datos de clientes y películas
        cursor.execute("SELECT customer_id FROM customer")
        customers = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT inventory_id, film_id FROM inventory")
        inventories = cursor.fetchall()
        inventory_dict = {row[0]: row[1] for row in inventories}
        inventory_ids = list(inventory_dict.keys())

        # Obtener las frecuencias de renta de clientes y películas
        cursor.execute(
            "SELECT customer_id, COUNT(*) AS rental_count FROM rental GROUP BY customer_id")
        customer_rental_counts = cursor.fetchall()
        customer_ids, customer_weights = zip(*customer_rental_counts)
        customer_ids = [int(id) for id in customer_ids]  # Convertir a int
        customer_weights = np.array(customer_weights) / sum(customer_weights)

        cursor.execute(
            "SELECT film_id, COUNT(*) AS rental_count FROM inventory INNER JOIN rental ON inventory.inventory_id = rental.inventory_id GROUP BY film_id")
        film_rental_counts = cursor.fetchall()
        film_ids, film_weights = zip(*film_rental_counts)
        film_ids = [int(id) for id in film_ids]  # Convertir a int
        film_weights = np.array(film_weights) / sum(film_weights)

        # Simular las rentas
        new_rentals = []
        for _ in range(100):
            customer_id = int(
                np.random.choice(
                    customer_ids,
                    p=customer_weights))
            film_id = int(np.random.choice(film_ids, p=film_weights))
            inventory_id = random.choice(
                [key for key, value in inventory_dict.items() if value == film_id])
            rental_date = datetime.now()
            return_date = rental_date + timedelta(days=random.randint(1, 7))

            new_rentals.append(
                (customer_id, inventory_id, rental_date, return_date))

        # Insertar las nuevas rentas en la base de datos
        insert_query = """
            INSERT INTO rental (customer_id, inventory_id, rental_date, return_date, staff_id, last_update)
            VALUES (%s, %s, %s, %s, 1, NOW())
        """
        cursor.executemany(insert_query, new_rentals)
        conn.commit()

        logger.info(f"Inserted {len(new_rentals)} new rentals.")

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
        'body': 'New rentals simulated and data uploaded to s3'
    }