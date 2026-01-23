from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, count, sum, when, broadcast, dense_rank

# 1. Number of movies per category
def query_1(tables):
    return (
        tables["film_category"]
        .join(broadcast(tables["category"].select("category_id", "name")), "category_id")
        .groupBy("name")
        .agg(count("film_id").alias("films_amount"))
        .withColumnRenamed("name", "category_name")
        .orderBy(col("films_amount").desc())
    )

# 2. Top 10 actors by total rentals
def query_2(tables):
    return (
        tables["rental"]
        .select("rental_id", "inventory_id")
        .join(tables["inventory"].select("inventory_id", "film_id"), "inventory_id")
        .join(tables["film_actor"].select("film_id", "actor_id"), "film_id")
        .join(broadcast(tables["actor"].select("actor_id", "first_name", "last_name")), "actor_id")
        .groupBy("actor_id", "first_name", "last_name")
        .agg(count("rental_id").alias("total_rentals"))
        .withColumn("actor_name", concat_ws(" ", col("first_name"), col("last_name")))
        .select("actor_name", "total_rentals")
        .orderBy(col("total_rentals").desc())
        .limit(10)
    )

# 3. Category with most money spent
def query_3(tables):
    payments = tables["payment"].select("rental_id", "amount")
    rentals = tables["rental"].select("rental_id", "inventory_id")
    inventory = tables["inventory"].select("inventory_id", "film_id")
    film_category = tables["film_category"].select("film_id", "category_id")
    category = broadcast(tables["category"].select("category_id", "name"))

    return (
        payments
        .join(rentals, "rental_id")
        .join(inventory, "inventory_id")
        .join(film_category, "film_id")
        .join(category, "category_id")
        .groupBy("category_id", "name")
        .agg(F.sum("amount").alias("total_spent"))
        .orderBy(col("total_spent").desc())
        .limit(1)
        .select(col("name"), col("total_spent"))
    )

# 4. Movies not in inventory
def query_4(tables):
    return (
        tables["film"]
        .select("film_id", "title")
        .join(tables["inventory"].select("film_id"), "film_id", "left_anti")
    )

# 5. Top 3 actors in "Children" category
def query_5(tables):
    children_category = broadcast(tables["category"].filter(col("name") == "Children").select("category_id", "name"))

    actor_counts = (
        tables["film_actor"].select("film_id", "actor_id")
        .join(tables["film"].select("film_id"), "film_id")
        .join(tables["film_category"].select("film_id", "category_id"), "film_id")
        .join(children_category, "category_id")
        .join(tables["actor"].select("actor_id", "first_name", "last_name"), "actor_id")
        .groupBy("actor_id", "first_name", "last_name", "name")
        .agg(count("film_id").alias("film_amount"))
        .withColumn("fullname", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumnRenamed("name", "category_name")
        .select("fullname", "category_name", "film_amount")
    )

    w = Window.partitionBy("category_name").orderBy(col("film_amount").desc())
    return actor_counts.withColumn("rank", dense_rank().over(w)).filter(col("rank") <= 3)

# 6. Cities with active/inactive customers
def query_6(tables):
    customer_address = tables["customer"].select("address_id", "active")
    city_table = broadcast(tables["city"].select("city_id", "city"))

    return (
        customer_address
        .join(tables["address"].select("address_id", "city_id"), "address_id")
        .join(city_table, "city_id")
        .groupBy("city")
        .agg(
            count(when(col("active") == 1, True)).alias("active_customers"),
            count(when(col("active") == 0, True)).alias("inactive_customers")
        )
        .orderBy(col("inactive_customers").desc())
    )

# 7. Top category by total rental hours per city
def query_7(tables):
    rental_df = (
        tables["rental"].select("rental_id", "customer_id", "inventory_id", "rental_date", "return_date")
    )
    customer_df = tables["customer"].select("customer_id", "address_id")
    address_df = tables["address"].select("address_id", "city_id")
    city_df = broadcast(tables["city"].select("city_id", "city"))
    inventory_df = tables["inventory"].select("inventory_id", "film_id")
    film_category_df = tables["film_category"].select("film_id", "category_id")
    category_df = broadcast(tables["category"].select("category_id", "name").withColumnRenamed("name", "category"))

    df = (
        rental_df
        .join(customer_df, "customer_id")
        .join(address_df, "address_id")
        .join(city_df, "city_id")
        .filter((col("city").rlike("^(?i)a")) | (col("city").contains("-")))
        .join(inventory_df, "inventory_id")
        .join(film_category_df, "film_id")
        .join(category_df, "category_id")
        .withColumn(
            "rental_hours",
            (F.unix_timestamp(F.coalesce("return_date", F.current_timestamp())) -
             F.unix_timestamp("rental_date")) / 3600
        )
        .groupBy("city", "category")
        .agg(F.sum("rental_hours").alias("total_hours"))
    )

    w = Window.partitionBy("city").orderBy(col("total_hours").desc())
    return df.withColumn("rank", F.rank().over(w)) \
             .filter(col("rank") == 1) \
             .select("city", "category", "total_hours")
