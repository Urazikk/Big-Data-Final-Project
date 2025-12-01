import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, when, sum, round, lit, abs
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

# 1. Initialisation
print("⏳ Initialisation de Spark...")
spark = SparkSession.builder \
    .appName("SportsAnalyticsEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schéma
schema = StructType() \
    .add("match_id", StringType()) \
    .add("league", StringType()) \
    .add("home_team", StringType()) \
    .add("away_team", StringType()) \
    .add("game_time", IntegerType()) \
    .add("event_type", StringType()) \
    .add("team", StringType()) \
    .add("xg", FloatType()) \
    .add("position_role", StringType())

# 3. Lecture Kafka
try:
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sports-events") \
        .option("startingOffsets", "latest") \
        .load()
except Exception as e:
    print(f"❌ Erreur Kafka : {e}")
    sys.exit(1)

# 4. Transformation
game_data = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --- AJOUT BIG DATA : ARCHIVAGE (DATA LAKE) ---
query_archival = game_data.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/raw_events") \
    .option("checkpointLocation", "data/checkpoints/raw_events") \
    .trigger(processingTime='1 minute') \
    .start()

# 5. Calculs "Tableau d'Affichage"
match_summary = game_data.groupBy("match_id", "league", "home_team", "away_team") \
    .agg(
        sum(when((col("event_type") == "GOAL") & (col("team") == col("home_team")), 1).otherwise(0)).alias("Score_Home"),
        sum(when((col("event_type") == "GOAL") & (col("team") == col("away_team")), 1).otherwise(0)).alias("Score_Away"),
        sum(when((col("event_type") == "PASS") & (col("team") == col("home_team")), 1).otherwise(0)).alias("Passes_Home"),
        sum(when((col("event_type") == "PASS") & (col("team") == col("away_team")), 1).otherwise(0)).alias("Passes_Away"),
        round(sum(when(col("team") == col("home_team"), col("xg")).otherwise(0)), 2).alias("xG_Home"),
        round(sum(when(col("team") == col("away_team"), col("xg")).otherwise(0)), 2).alias("xG_Away")
    )

# 6. Enrichissement Final (Possession) - SANS TENSION
final_scoreboard = match_summary.withColumn("Total_Passes", col("Passes_Home") + col("Passes_Away")) \
    .withColumn("Poss_Home", 
                when(col("Total_Passes") > 0, round((col("Passes_Home") / col("Total_Passes")) * 100, 1))
                .otherwise(50.0)) \
    .select(
        col("league"),
        col("home_team").alias("Domicile"),
        col("Score_Home").alias("Buts_D"),
        col("Score_Away").alias("Buts_E"),
        col("away_team").alias("Exterieur"),
        col("Poss_Home").alias("Poss%"),
        col("xG_Home"),
        col("xG_Away")
    ).orderBy("league")

# 7. Affichage
print(">>> ✅ Analytics V5 : Dashboard (Simplifié)")
print(">>> En attente de données...")

query_console = final_scoreboard.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n🛑 Arrêt Spark.")
    query_console.stop()
    query_archival.stop()