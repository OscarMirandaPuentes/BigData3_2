-- Alquileres de películas de los clientes
SELECT customer_key, COUNT(*) AS RentalCount 
FROM fact_rentals 
GROUP BY customer_key;

-- Comportamiento de los alquileres según las fechas

SELECT rental_date, COUNT(*) AS RentalsCount 
FROM fact_rentals 
GROUP BY rental_date;

-- Comportamiento de los alquileres según día de la semana

SELECT d.day_of_week, COUNT(*) AS RentalsCount
FROM fact_rentals r
JOIN dim_date d ON r.rental_date = d.date
GROUP BY d.day_of_week;

-- Categorías de las películas que más se alquilan

SELECT f.category, COUNT(*) AS RentalsCount 
FROM fact_rentals r
JOIN dim_film f ON r.film_key = f.film_id
GROUP BY f.category;

-- Clientes que más alquilan

SELECT 
    CONCAT(c.first_name, ' ', c.last_name) AS FullName,
    COUNT(r.rental_key) AS RentalCount
FROM 
    fact_rentals r
JOIN 
    dim_customer c ON r.customer_key = c.customer_id
GROUP BY 
    c.customer_id, FullName
ORDER BY 
    RentalCount DESC
LIMIT 10;