import mysql.connector
from datetime import datetime, timedelta

def populate_datawarehouse():
    # Conectar a la base de datos sakila
    conn_sakila = mysql.connector.connect(
        host='database-1.cjoy8kmcyakj.us-east-1.rds.amazonaws.com',
        user='admin',
        password='tres0916',
        database='sakila'
    )
    cursor_sakila = conn_sakila.cursor(dictionary=True)

    # Conectar a la base de datos datawarehouse_sakila
    conn_dw = mysql.connector.connect(
        host='database-1.cjoy8kmcyakj.us-east-1.rds.amazonaws.com',
        user='admin',
        password='tres0916',
        database='datawarehouse_sakila'
    )
    cursor_dw = conn_dw.cursor()

    # Poblar dim_customer
    cursor_sakila.execute("""
        SELECT c.customer_id, c.first_name, c.last_name, c.email, a.address, ci.city, co.country
        FROM customer c
        JOIN address a ON c.address_id = a.address_id
        JOIN city ci ON a.city_id = ci.city_id
        JOIN country co ON ci.country_id = co.country_id
    """)
    customers = cursor_sakila.fetchall()
    for customer in customers:
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
        """, (customer['customer_id'], customer['first_name'], customer['last_name'], customer['email'], customer['address'], customer['city'], customer['country']))
    conn_dw.commit()

    # Poblar dim_film
    cursor_sakila.execute("""
        SELECT f.film_id, f.title, f.description, f.release_year, l.name AS language, c.name AS category
        FROM film f
        JOIN language l ON f.language_id = l.language_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
    """)
    films = cursor_sakila.fetchall()
    for film in films:
        cursor_dw.execute("""
            INSERT INTO dim_film (film_id, title, description, release_year, language, category)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            description = VALUES(description),
            release_year = VALUES(release_year),
            language = VALUES(language),
            category = VALUES(category)
        """, (film['film_id'], film['title'], film['description'], film['release_year'], film['language'], film['category']))
    conn_dw.commit()

    # Poblar dim_date
    cursor_sakila.execute("""
        SELECT DISTINCT DATE(rental_date) AS date FROM rental
        UNION
        SELECT DISTINCT DATE(return_date) AS date FROM rental
    """)
    dates = cursor_sakila.fetchall()

    for date_record in dates:
        date = date_record['date']
        if date:
            date_key = date.strftime('%Y%m%d')
            day = date.day
            month = date.month
            year = date.year
            day_of_week = date.strftime('%A')
            is_weekend = date.weekday() >= 5
            quarter = (date.month - 1) // 3 + 1

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
            """, (date_key, date, day, month, year, day_of_week, False, is_weekend, quarter))
    conn_dw.commit()

    # Poblar fact_rentals
    cursor_sakila.execute("""
        SELECT r.rental_id, r.rental_date, r.inventory_id, r.customer_id, r.return_date, r.last_update, i.film_id
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
    """)
    rentals = cursor_sakila.fetchall()
    for rental in rentals:
        # Verificar existencia de customer_key y film_key
        cursor_dw.execute(
            "SELECT customer_key FROM dim_customer WHERE customer_id = %s",
            (rental['customer_id'],)
        )
        customer_key = cursor_dw.fetchone()

        cursor_dw.execute(
            "SELECT film_key FROM dim_film WHERE film_id = %s", (rental['film_id'],)
        )
        film_key = cursor_dw.fetchone()

        if customer_key and film_key:
            rental_date_key = rental['rental_date'].strftime('%Y%m%d') if rental['rental_date'] else None
            return_date_key = rental['return_date'].strftime('%Y%m%d') if rental['return_date'] else None

            cursor_dw.execute(
                """
                INSERT INTO fact_rentals (rental_date, customer_key, return_date, film_key, date_key)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (rental['rental_date'].strftime('%Y-%m-%d') if rental['rental_date'] else None,
                 customer_key[0],
                 rental['return_date'].strftime('%Y-%m-%d') if rental['return_date'] else None,
                 film_key[0],
                 rental_date_key))
    conn_dw.commit()

    cursor_sakila.close()
    conn_sakila.close()
    cursor_dw.close()
    conn_dw.close()

if __name__ == "__main__":
    populate_datawarehouse()
