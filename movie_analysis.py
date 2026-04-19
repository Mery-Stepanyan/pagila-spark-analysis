from pyspark.sql import SparkSession
import config
from utils import analysis_utils as au

spark = SparkSession.builder \
    .appName("PagilaProfessionalAnalysis") \
    .config("spark.jars", config.JAR_PATH) \
    .config("spark.driver.extraClassPath", config.JAR_PATH) \
    .getOrCreate()


tables = [
    "film", "film_category", "category", "actor", "film_actor", 
    "inventory", "rental", "payment", "customer", "address", "city"
]

try:
    dfs = {t: spark.read.jdbc(url=config.DB_URL, table=t, properties=config.DB_PROPERTIES) for t in tables}
    print("--- Database connection successful and all tables loaded ---")
except Exception as e:
    print(f"Error loading tables: {e}")
    spark.stop()
    exit(1)

# --- TASK 1: Number of movies in each category ---
print("\n--- TASK 1: MOVIES PER CATEGORY ---")
au.get_movies_per_category(dfs["film_category"], dfs["category"]).show()

# --- TASK 2: 10 actors whose movies rented the most ---
print("\n--- TASK 2: TOP 10 ACTORS BY RENTALS ---")
au.get_top_actors_by_rentals(dfs["actor"], dfs["film_actor"], dfs["inventory"], dfs["rental"]).show()

# --- TASK 3: Category with the most money spent ---
print("\n--- TASK 3: TOP CATEGORY BY REVENUE ---")
au.get_top_revenue_category(dfs["payment"], dfs["rental"], dfs["inventory"], dfs["film_category"], dfs["category"]).show()

# --- TASK 4: Movies not in the inventory ---
print("\n--- TASK 4: MOVIES NOT IN INVENTORY ---")
au.get_movies_not_in_inventory(dfs["film"], dfs["inventory"]).show()

# --- TASK 5: Top 3 actors in "Children" category ---
print("\n--- TASK 5: TOP 3 ACTORS IN CHILDREN CATEGORY ---")
au.get_top_children_actors(dfs["category"], dfs["film_category"], dfs["film_actor"], dfs["actor"]).show()

# --- TASK 6: Active and Inactive customers per city ---
print("\n--- TASK 6: ACTIVE/INACTIVE CUSTOMERS BY CITY ---")
au.get_customer_activity_by_city(dfs["customer"], dfs["address"], dfs["city"]).show()

# --- TASK 7: Rental hours analysis ---
print("\n--- TASK 7: TOP CATEGORY BY RENTAL HOURS (CITIES STARTING WITH 'A') ---")
au.get_rental_hours_analysis(
    dfs["rental"], dfs["inventory"], dfs["film_category"], dfs["category"], 
    dfs["customer"], dfs["address"], dfs["city"], "starts_with_a"
).show()

print("\n--- TASK 7: TOP CATEGORY BY RENTAL HOURS (CITIES WITH '-') ---")
au.get_rental_hours_analysis(
    dfs["rental"], dfs["inventory"], dfs["film_category"], dfs["category"], 
    dfs["customer"], dfs["address"], dfs["city"], "contains_dash"
).show()


spark.stop()