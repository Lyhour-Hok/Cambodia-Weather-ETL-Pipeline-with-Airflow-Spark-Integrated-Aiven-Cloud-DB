import json
import os
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, lower, trim, when
from dotenv import load_dotenv

# Load config ពី .env
load_dotenv()

def main():
    # 1. SETUP SPARK
    spark = SparkSession.builder.appName("CambodiaWeatherTransform").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. READ RAW DATA
    raw_path = "/tmp/weather_raw.json"
    if not os.path.exists(raw_path):
        print("❌ Error: Raw data not found!")
        return

    with open(raw_path, "r") as f:
        data = json.load(f)

    # 3. TRANSFORM WITH SPARK
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)

    df_clean = df.select(
        trim(col("city")).alias("city"),
        trim(col("province")).alias("province"),
        col("country"),
        col("latitude"),
        col("longitude"),
        spark_round(col("temperature"), 1).alias("temperature"),
        col("humidity").cast("int"),
        lower(trim(col("weather"))).alias("weather"),
        col("timestamp")
    ).withColumn(
        "heat_level",
        when(col("temperature") >= 38, "Extreme Heat")
        .when(col("temperature") >= 30, "Hot")
        .otherwise("Cool")
    )

    # 4. PREPARE DATA FOR DATABASE
    # ប្តូរពី DataFrame ទៅជា list នៃ tuples ដើម្បីស្រួល Insert
    rows_to_upload = [tuple(row) for row in df_clean.collect()]

    # 5. LOAD TO AIVEN CLOUD DB
    print("🚀 Connecting to Aiven Cloud...")
    try:
        db_conn = mysql.connector.connect(
            host=os.getenv("AIVEN_HOST"),
            port=os.getenv("AIVEN_PORT"),
            user=os.getenv("AIVEN_USER"),
            password=os.getenv("AIVEN_PASSWORD"),
            database=os.getenv("AIVEN_DB")
        )
        cursor = db_conn.cursor()

        # បង្កើត Table (Schema សាមញ្ញសម្រាប់ Test)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cambodia_weather (
                city VARCHAR(100), province VARCHAR(100), country VARCHAR(50),
                latitude DOUBLE, longitude DOUBLE, temperature DOUBLE,
                humidity INT, weather VARCHAR(100), timestamp DATETIME,
                heat_level VARCHAR(50)
            )
        """)

        # លុបទិន្នន័យចាស់ចោលមុននឹង Load ថ្មី (ដើម្បីកុំឱ្យជាន់គ្នា)
        cursor.execute("TRUNCATE TABLE cambodia_weather")

        # Insert ទិន្នន័យទាំងអស់ចូលម្តងគត់ (Fast Way)
        sql = "INSERT INTO cambodia_weather VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(sql, rows_to_upload)

        db_conn.commit()
        print(f"✅ World-class! Loaded {len(rows_to_upload)} provinces to Cloud DB.")

    except Exception as e:
        print(f"❌ DB Error: {e}")
    finally:
        if 'db_conn' in locals() and db_conn.is_connected():
            cursor.close()
            db_conn.close()
        spark.stop()

if __name__ == "__main__":
    main()