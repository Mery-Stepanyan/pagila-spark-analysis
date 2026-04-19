from pyspark.sql import functions as F
from pyspark.sql.window import Window

def get_movies_per_category(df_film_category, df_category):
    return df_film_category.join(df_category, "category_id") \
        .groupBy("name").agg(F.count("film_id").alias("movie_count")) \
        .orderBy(F.desc("movie_count"))

def get_top_actors_by_rentals(df_actor, df_film_actor, df_inventory, df_rental):
    return df_actor.join(df_film_actor, "actor_id").join(df_inventory, "film_id") \
        .join(df_rental, "inventory_id").groupBy("actor_id", "first_name", "last_name") \
        .agg(F.count("rental_id").alias("total_rentals")) \
        .orderBy(F.desc("total_rentals")).limit(10)

def get_top_revenue_category(df_payment, df_rental, df_inventory, df_film_category, df_category):
    return df_payment.join(df_rental, "rental_id").join(df_inventory, "inventory_id") \
        .join(df_film_category, "film_id").join(df_category, "category_id") \
        .groupBy("name").agg(F.sum("amount").alias("total_spent")) \
        .orderBy(F.desc("total_spent")).limit(1)

def get_movies_not_in_inventory(df_film, df_inventory):
    return df_film.join(df_inventory, "film_id", "left_anti").select("title")

def get_top_children_actors(df_category, df_film_category, df_film_actor, df_actor):
    children_actors = df_category.filter(F.col("name") == "Children") \
        .join(df_film_category, "category_id").join(df_film_actor, "film_id").join(df_actor, "actor_id") \
        .groupBy("actor_id", "first_name", "last_name").agg(F.count("film_id").alias("movie_count"))
    window_spec = Window.orderBy(F.desc("movie_count"))
    return children_actors.withColumn("rank", F.dense_rank().over(window_spec)).filter(F.col("rank") <= 3)

def get_customer_activity_by_city(df_customer, df_address, df_city):
    return df_customer.join(df_address, "address_id").join(df_city, "city_id") \
        .groupBy("city").agg(
            F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_count"),
            F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_count")
        ).orderBy(F.desc("inactive_count"))

def get_rental_hours_analysis(df_rental, df_inventory, df_film_category, df_category, df_customer, df_address, df_city, filter_type):
    df_hours = df_rental.join(df_inventory, "inventory_id").join(df_film_category, "film_id") \
        .join(df_category, "category_id").join(df_customer, "customer_id") \
        .join(df_address, "address_id").join(df_city, "city_id") \
        .withColumn("hours", (F.unix_timestamp("return_date") - F.unix_timestamp("rental_date")) / 3600)
    
    if filter_type == "starts_with_a":
        return df_hours.filter(F.col("city").ilike("a%")).groupBy("name") \
            .agg(F.sum("hours").alias("total_hours")).orderBy(F.desc("total_hours")).limit(1)
    else: 
        return df_hours.filter(F.col("city").contains("-")).groupBy("name") \
            .agg(F.sum("hours").alias("total_hours")).orderBy(F.desc("total_hours")).limit(1)