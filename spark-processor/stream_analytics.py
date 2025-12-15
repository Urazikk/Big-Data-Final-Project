import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, when, sum, round, window, desc, lit
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType

# 1. Initialisation
print("⏳ Initialisation de Spark...")

# Configuration Spark avec option pour Windows (LocalFileSystem)
# L'option spark.hadoop.fs.file.impl est critique pour éviter l'erreur UnsatisfiedLinkError sur Windows
spark = SparkSession.builder \
    .appName("SportsAnalyticsEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schéma
schema = StructType() \
    .add("match_id", StringType()) \
    .add("league", StringType()) \
    .add("home_team", StringType()) \
    .add("away_team", StringType()) \
    .add("timestamp", IntegerType()) \
    .add("game_time", IntegerType()) \
    .add("event_type", StringType()) \
    .add("team", StringType()) \
    .add("player", StringType()) \
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
# On cast le timestamp integer en vrai TimestampType pour les fenêtres
game_data = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("event_time", col("timestamp").cast(TimestampType()))

# --- CALCUL DU SCORE & MVP ---

# A. Score et Stats de base (Global)
match_summary = game_data.groupBy("match_id", "league", "home_team", "away_team") \
    .agg(
        sum(when((col("event_type") == "GOAL") & (col("team") == col("home_team")), 1).otherwise(0)).alias("Score_Home"),
        sum(when((col("event_type") == "GOAL") & (col("team") == col("away_team")), 1).otherwise(0)).alias("Score_Away"),
        round(sum(when(col("team") == col("home_team"), col("xg")).otherwise(0)), 2).alias("xG_Home"),
        round(sum(when(col("team") == col("away_team"), col("xg")).otherwise(0)), 2).alias("xG_Away")
    )

# C. MVP en temps réel (Le joueur le plus actif/dangereux)
mvp_df = game_data \
    .groupBy("match_id", "player", "team") \
    .agg(
        (sum(when(col("event_type") == "GOAL", 50)
             .when(col("event_type") == "SHOT", 10)
             .when(col("event_type") == "PASS", 2)
             .otherwise(1))).alias("player_impact")
    ) \
    .orderBy(desc("player_impact")) \
    .limit(5) # Top 5 joueurs tout match confondu

# 5. Assemblage final (Jointures simplifiées pour affichage console)

final_board = match_summary.select(
    col("league"),
    col("home_team").alias("Home"),
    col("Score_Home").alias("Goals_Home"),
    col("Score_Away").alias("Goals_Away"),
    col("away_team").alias("Away"),
    col("xG_Home"),
    col("xG_Away")
).orderBy("league")

# 7. Affichage
print(">>> ✅ Analytics V9 : Dashboard (English)")
print(">>> Waiting for data stream...")

# Vue 1 : Scores (Mise à jour toutes les 10 secondes)
query_console = final_board.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .start()

# Vue 2 : Top Players (MVP Race) (Mise à jour toutes les 20 secondes)
query_mvp = mvp_df.select(
    col("player").alias("Player"),
    col("team").alias("Team"),
    col("player_impact").alias("Impact_Score")
).writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='20 seconds') \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping Spark.")
    query_console.stop()
    query_mvp.stop()