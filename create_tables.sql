-- Crear la tabla dim_customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    address VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    UNIQUE (customer_id)
);

-- Crear la tabla dim_film
CREATE TABLE IF NOT EXISTS dim_film (
    film_key INT AUTO_INCREMENT PRIMARY KEY,
    film_id INT NOT NULL,
    title VARCHAR(255),
    description TEXT,
    release_year YEAR,
    language VARCHAR(50),
    category VARCHAR(50),
    UNIQUE (film_id)
);

-- Crear la tabla dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(20),
    is_holiday BOOLEAN,
    is_weekend BOOLEAN,
    quarter INT
);

-- Crear la tabla fact_rentals
CREATE TABLE IF NOT EXISTS fact_rentals (
    rental_key INT AUTO_INCREMENT PRIMARY KEY,
    rental_date DATE,
    customer_key INT,
    return_date DATE,
    film_key INT,
    date_key INT,
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (film_key) REFERENCES dim_film(film_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);