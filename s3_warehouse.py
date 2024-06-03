import sys
import boto3
import pandas as pd
import holidays
import mysql.connector
from datetime import datetime
import logging
import io

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Parámetros de conexión a la base de datos de data warehouse
dw_url = "jdbc:mysql://database-1.cjoy8kmcyakj.us-east-1.rds.amazonaws.com:3306/datawarehouse_sakila"
dw_properties = {
    "user": "admin",
    "password": "tres0916",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Leer datos de S3
logger.info("Reading data from S3")
s3_client = boto3.client('s3')
bucket_name = "sakila-warehouse-2"
landing_prefix = "landing/"

# Leer archivos CSV desde S3


def read_csv_from_s3(bucket, key):
    csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    return pd.read_csv(io.StringIO(csv_string))


customers_df = read_csv_from_s3(bucket_name, landing_prefix + "customer.csv")
category_df = read_csv_from_s3(bucket_name, landing_prefix + "category.csv")
rental_df = read_csv_from_s3(bucket_name, landing_prefix + "rental.csv")
film_df = read_csv_from_s3(bucket_name, landing_prefix + "film.csv")
inventory_df = read_csv_from_s3(bucket_name, landing_prefix + "inventory.csv")
address_df = read_csv_from_s3(bucket_name, landing_prefix + "address.csv")
city_df = read_csv_from_s3(bucket_name, landing_prefix + "city.csv")
country_df = read_csv_from_s3(bucket_name, landing_prefix + "country.csv")

# Preprocesar DataFrames


def preprocess_dataframe(df, columns):
    df.fillna({col: '' for col in columns}, inplace=True)
    df = df.astype({col: str for col in columns})
    return df


customers_df = preprocess_dataframe(customers_df, customers_df.columns)
category_df = preprocess_dataframe(category_df, category_df.columns)
rental_df = preprocess_dataframe(rental_df, rental_df.columns)
film_df = preprocess_dataframe(film_df, film_df.columns)
inventory_df = preprocess_dataframe(inventory_df, inventory_df.columns)
address_df = preprocess_dataframe(address_df, address_df.columns)
city_df = preprocess_dataframe(city_df, city_df.columns)
country_df = preprocess_dataframe(country_df, country_df.columns)

# Convertir columnas de fecha a formato de fecha correcto


def convert_to_date(df, columns):
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    return df


rental_df = convert_to_date(rental_df, ['rental_date', 'return_date'])

# Convertir columnas de enteros, manejando valores vacíos


def convert_to_int(df, columns):
    for col in columns:
        df[col] = df[col].apply(lambda x: int(x) if str(x).isdigit() else 0)
    return df


customers_df = convert_to_int(
    customers_df, [
        'customer_id', 'address_id', 'active'])
rental_df = convert_to_int(
    rental_df, [
        'rental_id', 'inventory_id', 'customer_id', 'staff_id'])
film_df = convert_to_int(film_df,
                         ['film_id',
                          'release_year',
                          'language_id',
                          'original_language_id',
                          'rental_duration',
                          'length'])
inventory_df = convert_to_int(
    inventory_df, [
        'inventory_id', 'film_id', 'store_id'])
address_df = convert_to_int(address_df, ['address_id', 'city_id'])
city_df = convert_to_int(city_df, ['city_id', 'country_id'])
country_df = convert_to_int(country_df, ['country_id'])
category_df = convert_to_int(category_df, ['category_id'])

# Join de tablas para obtener nombres de ciudad y país en dim_customer
customers_df = customers_df.merge(
    address_df[['address_id', 'address', 'city_id']], on='address_id', how='left')
customers_df = customers_df.merge(
    city_df[['city_id', 'city', 'country_id']], on='city_id', how='left')
customers_df = customers_df.merge(
    country_df[['country_id', 'country']], on='country_id', how='left')


# JOINS
# Unimos fact_rentals con dim_customer para obtener los nombres del cliente
# Unimos fact_rentals con dim_customer para obtener los nombres del cliente
rental_df = rental_df.merge(customers_df[['customer_id', 'first_name', 'last_name']],
                            on='customer_id',
                            how='left')

# Unimos el resultado anterior con inventory_df para obtener el film_id
rental_df = rental_df.merge(inventory_df[['inventory_id', 'film_id']],
                            on='inventory_id',
                            how='left')

# Unimos el resultado anterior con dim_film para obtener el título de la
# película
rental_df = rental_df.merge(film_df[['film_id', 'title']],
                            on='film_id',
                            how='left')


# Convertir todas las columnas a tipos nativos de Python
customers_df = customers_df.astype(
    object).where(pd.notnull(customers_df), None)
category_df = category_df.astype(object).where(pd.notnull(category_df), None)
rental_df = rental_df.astype(object).where(pd.notnull(rental_df), None)
film_df = film_df.astype(object).where(pd.notnull(film_df), None)
inventory_df = inventory_df.astype(
    object).where(pd.notnull(inventory_df), None)
address_df = address_df.astype(object).where(pd.notnull(address_df), None)
city_df = city_df.astype(object).where(pd.notnull(city_df), None)
country_df = country_df.astype(object).where(pd.notnull(country_df), None)

# Mostrar los esquemas para verificar las columnas
logger.info("Customer DataFrame Schema:")
logger.info(customers_df.head())
logger.info("Rental DataFrame Schema:")
logger.info(rental_df.head())
logger.info("Film DataFrame Schema:")
logger.info(film_df.head())
logger.info("Inventory DataFrame Schema:")
logger.info(inventory_df.head())

# Conectar a la base de datos datawarehouse_sakila
conn_dw = mysql.connector.connect(
    host='database-1.cjoy8kmcyakj.us-east-1.rds.amazonaws.com',
    user='admin',
    password='tres0916',
    database='datawarehouse_sakila'
)
cursor_dw = conn_dw.cursor()

# Función para vaciar las tablas


def delete_table_data(cursor, table_name):
    cursor.execute(f"DELETE FROM {table_name}")


# Poblar dim_date con todas las fechas de rental_date y return_date
all_dates = set(
    rental_df['rental_date'].dropna()) | set(
        rental_df['return_date'].dropna())
us_holidays = holidays.US(years=range(2000, 2025))

# Función para poblar las tablas de datawarehouse


def populate_datawarehouse():
    # Poblar dim_customer
    for index, row in customers_df.iterrows():
        cursor_dw.execute("""
            INSERT INTO dim_customer (customer_id, first_name, last_name, email, address, city, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            email = VALUES(email),
            address = VALUES(address),
            city = VALUES(city),
            country = VALUES(country)
        """, (int(row['customer_id']), row['first_name'], row['last_name'], row['email'], row['address'], row['city'], row['country']))
    conn_dw.commit()

    # Poblar dim_category
    for index, row in category_df.iterrows():
        cursor_dw.execute("""
            INSERT INTO dim_category (category_id, name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            name = VALUES(name)
        """, (int(row['category_id']), row['name']))
    conn_dw.commit()

    # Poblar dim_film
    for index, row in film_df.iterrows():
        cursor_dw.execute("""
            INSERT INTO dim_film (film_id, title, description, release_year, language, category)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            description = VALUES(description),
            release_year = VALUES(release_year),
            language = VALUES(language),
            category = VALUES(category)
        """, (int(row['film_id']), row['title'], row['description'], int(row['release_year']), int(row['language_id']), int(row['original_language_id'])))
    conn_dw.commit()

    for date in all_dates:
        if date:
            cursor_dw.execute("""
                INSERT INTO dim_date (date_key, date, day, month, year, day_of_week, is_holiday, is_weekend, quarter)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                day = VALUES(day),
                month = VALUES(month),
                year = VALUES(year),
                day_of_week = VALUES(day_of_week),
                is_holiday = VALUES(is_holiday),
                is_weekend = VALUES(is_weekend),
                quarter = VALUES(quarter)
            """, (
                date.strftime('%Y%m%d'),
                date,
                date.day,
                date.month,
                date.year,
                date.strftime('%A'),
                date in us_holidays,
                date.weekday() >= 5,
                (date.month - 1) // 3 + 1
            ))
    conn_dw.commit()

    # Poblar fact_rentals
    for index, row in rental_df.iterrows():
        if pd.isnull(row['rental_date']) or pd.isnull(row['return_date']):
            continue

        cursor_dw.execute("""
            SELECT customer_key FROM dim_customer WHERE customer_id = %s
        """, (int(row['customer_id']),))
        customer_key = cursor_dw.fetchone()

        # Obtener el film_id desde inventory_df
        film_id_row = inventory_df[inventory_df['inventory_id'] == int(
            row['inventory_id'])]
        if film_id_row.empty:
            logger.warning(
                f"No film_id found for inventory_id {row['inventory_id']}")
            continue
        film_id = film_id_row['film_id'].values[0]

        cursor_dw.execute("""
            SELECT film_key FROM dim_film WHERE film_id = %s
        """, (int(film_id),))
        film_key = cursor_dw.fetchone()

        cursor_dw.execute("""
            SELECT date_key FROM dim_date WHERE date = %s
        """, (row['rental_date'],))
        rental_date_key = cursor_dw.fetchone()

        cursor_dw.execute("""
            SELECT date_key FROM dim_date WHERE date = %s
        """, (row['return_date'],))
        return_date_key = cursor_dw.fetchone()

        if customer_key and film_key and rental_date_key and return_date_key:
            try:
                cursor_dw.execute("""
                    INSERT INTO fact_rentals (rental_date, customer_key, return_date, film_key, date_key,customer_name, film_title)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['rental_date'],
                    customer_key[0],
                    row['return_date'],
                    film_key[0],
                    rental_date_key[0],
                    row['first_name'] + ' ' + row['last_name'],
                    row['title']
                ))
            except Exception as e:
                logger.error(f"Error inserting rental record {index}: {e}")
    conn_dw.commit()

    # Cerrar conexiones
    cursor_dw.close()
    conn_dw.close()


# Vaciar las tablas
cursor_dw.execute("SET FOREIGN_KEY_CHECKS = 0")
delete_table_data(cursor_dw, 'fact_rentals')
delete_table_data(cursor_dw, 'dim_customer')
delete_table_data(cursor_dw, 'dim_film')
delete_table_data(cursor_dw, 'dim_date')
delete_table_data(cursor_dw, 'dim_category')
cursor_dw.execute("SET FOREIGN_KEY_CHECKS = 1")
conn_dw.commit()

# Poblar las tablas del data warehouse
populate_datawarehouse()

# Guardar los datos en S3 particionados por día
output_bucket = "sakila-warehouse-2"
output_prefix = "final/"


def write_to_s3(df, bucket, prefix, partition_col):
    df['date_partition'] = pd.to_datetime(
        df[partition_col], errors='coerce').dt.strftime('%Y-%m-%d')
    for date, group in df.groupby('date_partition'):
        # Definir el nombre del archivo
        file_name = f"{date}.parquet"
        output_path = f"{prefix}{date}/{file_name}"
        buffer = io.BytesIO()
        group.to_parquet(buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=output_path,
            Body=buffer.getvalue())


# Llama a la función para cada DataFrame
write_to_s3(
    customers_df,
    output_bucket,
    output_prefix +
    "dim_customer/",
    'create_date')
write_to_s3(
    rental_df,
    output_bucket,
    output_prefix +
    "fact_rentals/",
    'rental_date')
write_to_s3(film_df, output_bucket, output_prefix + "dim_film/", 'last_update')
write_to_s3(
    category_df,
    output_bucket,
    output_prefix +
    "dim_category/",
    'last_update')

# Crear un DataFrame para dim_date
dim_date_df = pd.DataFrame({
    'date_key': [d.strftime('%Y%m%d') for d in all_dates],
    'date': [d for d in all_dates],
    'day': [d.day for d in all_dates],
    'month': [d.month for d in all_dates],
    'year': [d.year for d in all_dates],
    'day_of_week': [d.strftime('%A') for d in all_dates],
    'is_holiday': [d in us_holidays for d in all_dates],
    'is_weekend': [d.weekday() >= 5 for d in all_dates],
    'quarter': [(d.month - 1) // 3 + 1 for d in all_dates]
})

write_to_s3(dim_date_df, output_bucket, output_prefix + "dim_date/", 'date')

logger.info("Data successfully written to S3.")