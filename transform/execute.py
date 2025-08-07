import os
import sys
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

# Import format_time utility or define it here
def format_time(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

def setup_logger(log_file_name="transform.log"):
    logger = logging.getLogger("transform_logger")
    if logger.hasHandlers():
        # Avoid adding handlers multiple times
        return logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s'
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file_name)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def create_spark_session(logger):
    """Initialize Spark session."""
    logger.debug("Initializing Spark session for transformation")
    return SparkSession.builder.appName("SpotifyDataTransform").getOrCreate()

def load_and_clean(spark, input_dir, output_dir, logger):
    """Stage 1: Load data, drop duplicates, remove nulls, save cleaned data."""
    try:
        artists_schema = T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("followers", T.FloatType(), True),
            T.StructField("genres", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("popularity", T.IntegerType(), True)
        ])

        recommendations_schema = T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("related_ids", T.ArrayType(T.StringType()), True)
        ])

        tracks_schema = T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("popularity", T.IntegerType(), True),
            T.StructField("duration_ms", T.IntegerType(), True),
            T.StructField("explicit", T.IntegerType(), True),
            T.StructField("artists", T.StringType(), True),
            T.StructField("id_artists", T.StringType(), True),
            T.StructField("release_date", T.StringType(), True),
            T.StructField("danceability", T.FloatType(), True),
            T.StructField("energy", T.FloatType(), True),
            T.StructField("key", T.IntegerType(), True),
            T.StructField("loudness", T.FloatType(), True),
            T.StructField("mode", T.IntegerType(), True),
            T.StructField("speechiness", T.FloatType(), True),
            T.StructField("acousticness", T.FloatType(), True),
            T.StructField("instrumentalness", T.FloatType(), True),
            T.StructField("liveness", T.FloatType(), True),
            T.StructField("valence", T.FloatType(), True),
            T.StructField("tempo", T.FloatType(), True),
            T.StructField("time_signature", T.IntegerType(), True)
        ])

        logger.debug(f"Reading artists CSV from {input_dir}")
        artists_df = spark.read.schema(artists_schema).csv(os.path.join(input_dir, "artists.csv"), header=True)
        logger.debug(f"Reading recommendations JSON from {input_dir}")
        recommendations_df = spark.read.schema(recommendations_schema).json(os.path.join(input_dir, "fixed_da.json"))
        logger.debug(f"Reading tracks CSV from {input_dir}")
        tracks_df = spark.read.schema(tracks_schema).csv(os.path.join(input_dir, "tracks.csv"), header=True)

        artists_df = artists_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
        recommendations_df = recommendations_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
        tracks_df = tracks_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())

        stage1_path = os.path.join(output_dir, "stage1")
        artists_df.write.mode("overwrite").parquet(os.path.join(stage1_path, "artists"))
        recommendations_df.write.mode("overwrite").parquet(os.path.join(stage1_path, "recommendations"))
        tracks_df.write.mode("overwrite").parquet(os.path.join(stage1_path, "tracks"))

        logger.info("Stage 1: Cleaned data saved")
    except Exception as e:
        logger.error(f"Error in load_and_clean: {e}")
        sys.exit(1)

    return artists_df, recommendations_df, tracks_df

def create_master_table(output_dir, artists_df, recommendations_df, tracks_df, logger):
    """Stage 2: Create master table by joining artists, tracks, and recommendations."""
    try:
        tracks_df = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists"), T.ArrayType(T.StringType())))
        tracks_exploded = tracks_df.select("id", "name", "popularity", "id_artists_array") \
                                   .withColumn("artist_id", F.explode("id_artists_array"))

        master_df = tracks_exploded.join(artists_df, tracks_exploded.artist_id == artists_df.id, "left") \
            .select(
                tracks_exploded["id"].alias("track_id"),
                tracks_exploded["name"].alias("track_name"),
                tracks_exploded["popularity"].alias("track_popularity"),
                artists_df["id"].alias("artist_id"),
                artists_df["name"].alias("artist_name"),
                artists_df["followers"],
                artists_df["genres"],
                artists_df["popularity"].alias("artist_popularity")
            )

        master_df = master_df.join(recommendations_df, master_df.artist_id == recommendations_df.id, "left") \
            .select(
                master_df["track_id"],
                master_df["track_name"],
                master_df["track_popularity"],
                master_df["artist_id"],
                master_df["artist_name"],
                master_df["followers"],
                master_df["genres"],
                master_df["artist_popularity"],
                recommendations_df["related_ids"]
            )

        stage2_path = os.path.join(output_dir, "stage2")
        master_df.write.mode("overwrite").parquet(os.path.join(stage2_path, "master_table"))
        logger.info("Stage 2: Master table saved")

    except Exception as e:
        logger.error(f"Error in create_master_table: {e}")
        sys.exit(1)

def create_query_tables(output_dir, artists_df, recommendations_df, tracks_df, logger):
    """Stage 3: Create query-optimized tables."""
    try:
        recommendations_exploded = recommendations_df.withColumn("related_id", F.explode("related_ids")) \
            .select("id", "related_id")

        stage3_path = os.path.join(output_dir, "stage3")

        recommendations_exploded.write.mode("overwrite").parquet(os.path.join(stage3_path, "recommendations_exploded"))

        tracks_df = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists"), T.ArrayType(T.StringType())))
        tracks_exploded = tracks_df.withColumn("artist_id", F.explode("id_artists_array")) \
            .select("id", "artist_id")

        tracks_exploded.write.mode("overwrite").parquet(os.path.join(stage3_path, "artist_track"))

        tracks_metadata = tracks_df.select(
            "id", "name", "popularity", "duration_ms", "danceability", "energy", "tempo"
        )
        tracks_metadata.write.mode("overwrite").parquet(os.path.join(stage3_path, "track_metadata"))

        artists_metadata = artists_df.select("id", "name", "followers", "popularity")
        artists_metadata.write.mode("overwrite").parquet(os.path.join(stage3_path, "artist_metadata"))

        logger.info("Stage 3: Query-optimized tables saved")

    except Exception as e:
        logger.error(f"Error in create_query_tables: {e}")
        sys.exit(1)

if __name__ == "__main__":
    logger = setup_logger()
    logger.info(f"Script started with args: {sys.argv}")

    if len(sys.argv) != 3:
        logger.error(f"Invalid number of arguments: {len(sys.argv)} received; expected 3 (script, input_dir, output_dir)")
        logger.error("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.exists(input_dir):
        logger.error(f"Input directory {input_dir} does not exist")
        sys.exit(1)

    if not os.path.exists(output_dir):
        logger.info(f"Output directory {output_dir} does not exist, creating it.")
        os.makedirs(output_dir)

    start = time.time()
    spark = create_spark_session(logger)

    artists_df, recommendations_df, tracks_df = load_and_clean(spark, input_dir, output_dir, logger)
    create_master_table(output_dir, artists_df, recommendations_df, tracks_df, logger)
    create_query_tables(output_dir, artists_df, recommendations_df, tracks_df, logger)

    end = time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken {format_time(end - start)}")

    spark.stop()
