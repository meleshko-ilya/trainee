-- 1. Output the number of movies in each category, sorted descending.

SELECT category.name AS category_name,
       COUNT(title)  AS films_amount
FROM film
         INNER JOIN film_category ON film_category.film_id = film.film_id
         INNER JOIN category ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY films_amount DESC;

-- 2. Output the 10 actors whose movies rented the most, sorted in descending order.

SELECT CONCAT(actor.first_name, ' ', actor.last_name) AS actor_name,
       COUNT(rental.rental_id)                        AS total_rentals
FROM rental
         INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
         INNER JOIN film ON inventory.film_id = film.film_id
         INNER JOIN film_actor ON film.film_id = film_actor.film_id
         INNER JOIN actor ON film_actor.actor_id = actor.actor_id
GROUP BY actor.actor_id
ORDER BY total_rentals DESC
LIMIT 10;

-- 3. Output the category of movies on which the most money was spent.

SELECT category.name,
       SUM(payment.amount) AS total_spent
FROM payment
         INNER JOIN rental ON rental.rental_id = payment.rental_id
         INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
         INNER JOIN film ON inventory.film_id = film.film_id
         INNER JOIN film_category ON film_category.film_id = film.film_id
         INNER JOIN category ON category.category_id = film_category.category_id
GROUP BY category.category_id
ORDER BY total_spent DESC
LIMIT 1;

-- 4. Print the names of movies that are not in the inventory. Write a query without using the IN operator

SELECT film.title
FROM film
         LEFT JOIN inventory ON film.film_id = inventory.film_id
WHERE inventory.inventory_id IS NULL;

-- 5. Output the top 3 actors who have appeared the most in movies in the “Children” category. If several actors have the same number of movies, output all of them.

WITH actor_counts AS (SELECT CONCAT(actor.first_name, ' ', actor.last_name) AS fullname,
                             category.name                                  AS category_name,
                             COUNT(film.film_id)                            AS film_amount
                      FROM actor
                               INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
                               INNER JOIN film ON film_actor.film_id = film.film_id
                               INNER JOIN film_category ON film_category.film_id = film.film_id
                               INNER JOIN category ON category.category_id = film_category.category_id
                      WHERE category.name = 'Children'
                      GROUP BY actor.actor_id, category.category_id
                      ORDER BY film_amount DESC),
     ranked AS (SELECT *, DENSE_RANK() OVER (ORDER BY film_amount DESC) AS rank
                FROM actor_counts)
SELECT fullname, category_name, film_amount, rank
FROM ranked
WHERE rank <= 3;

-- 6. Output cities with the number of active and inactive customers (active - customer.active = 1). Sort by the number of inactive customers in descending order.

SELECT city.city,
       SUM(CASE
               WHEN customer.active = 1
                   THEN 1
               ELSE 0
           END) AS active_customers,
       SUM(CASE
               WHEN customer.active = 0
                   THEN 1
               ELSE 0
           END) AS inactive_cusomers
FROM customer
         INNER JOIN address ON address.address_id = customer.address_id
         INNER JOIN city ON city.city_id = address.city_id
GROUP BY city.city
ORDER BY inactive_cusomers DESC;

-- 7. Output the category of movies that have the highest number of total rental hours in the city
-- (customer.address_id in this city) and that start with the letter “a”. Do the same for cities that have a “-” in them. Write everything in one query.

SELECT city, category, total_hours
FROM (SELECT city.city,
             category.name                                                                              AS category,
             SUM(EXTRACT(EPOCH FROM (COALESCE(rental.return_date, NOW()) - rental.rental_date)) / 3600) AS total_hours,
             RANK() OVER (
                 PARTITION BY city.city
                 ORDER BY SUM(EXTRACT(EPOCH FROM (COALESCE(rental.return_date, NOW()) - rental.rental_date)) /
                              3600) DESC
                 )                                                                                      AS rank
      FROM city
               JOIN address ON city.city_id = address.city_id
               JOIN customer ON address.address_id = customer.address_id
               JOIN rental ON customer.customer_id = rental.customer_id
               JOIN inventory ON rental.inventory_id = inventory.inventory_id
               JOIN film ON inventory.film_id = film.film_id
               JOIN film_category ON film.film_id = film_category.film_id
               JOIN category ON film_category.category_id = category.category_id
      WHERE city.city ILIKE 'a%'
         OR city.city LIKE '%-%'
      GROUP BY city.city, category.name) total
WHERE rank = 1;
