# Databricks notebook source
print("Currency ETL project started successfully")

# COMMAND ----------

import requests

url = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,CHF,JPY,CAD,AUD"

response = requests.get(url)

print(response.status_code)
print(response.json())

# COMMAND ----------

api_data = response.json()

currency_df = spark.createDataFrame(api_data)

display(currency_df)

# COMMAND ----------

currency_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_timestamp

clean_currency_df = (
    currency_df
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("extracted_at", current_timestamp())
)

display(clean_currency_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when

clean_currency_df.select(
    [
        sum(when(col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in clean_currency_df.columns
    ]
).show()

# COMMAND ----------

total_rows = clean_currency_df.count()

unique_rows = clean_currency_df.select(
    "date", "base", "quote"
).distinct().count()

print("Total rows:", total_rows)
print("Unique currency records:", unique_rows)
print("Duplicate rows:", total_rows - unique_rows)

# COMMAND ----------

final_currency_df = clean_currency_df.dropDuplicates(
    ["date", "base", "quote"]
)

print("Rows after duplicate removal:", final_currency_df.count())

display(final_currency_df)

# COMMAND ----------

final_currency_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("currency_exchange_rates")

# COMMAND ----------

saved_table_df = spark.table("currency_exchange_rates")

display(saved_table_df)

# COMMAND ----------

historical_url = (
    "https://api.frankfurter.dev/v2/rates"
    "?from=2025-01-01"
    "&base=EUR"
    "&quotes=USD,GBP,CHF,JPY,CAD,AUD"
)

historical_response = requests.get(historical_url)

print(historical_response.status_code)
print("Number of records:", len(historical_response.json()))

# COMMAND ----------

historical_data = historical_response.json()

historical_currency_df = spark.createDataFrame(historical_data)

display(historical_currency_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_timestamp

clean_historical_df = (
    historical_currency_df
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("extracted_at", current_timestamp())
)

display(clean_historical_df)

# COMMAND ----------

clean_historical_df.select(
    [
        sum(when(col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in clean_historical_df.columns
    ]
).show()

# COMMAND ----------

historical_total_rows = clean_historical_df.count()

historical_unique_rows = clean_historical_df.select(
    "date", "base", "quote"
).distinct().count()

print("Total rows:", historical_total_rows)
print("Unique currency records:", historical_unique_rows)
print("Duplicate rows:", historical_total_rows - historical_unique_rows)

# COMMAND ----------

final_historical_df = clean_historical_df.dropDuplicates(
    ["date", "base", "quote"]
)

print("Rows after cleaning:", final_historical_df.count())

# COMMAND ----------

final_historical_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_exchange_rates")

# COMMAND ----------

silver_df = spark.table("silver_exchange_rates")

print("Silver table rows:", silver_df.count())

display(silver_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, round

currency_window = Window.partitionBy("quote").orderBy("date")

gold_currency_df = (
    silver_df
    .withColumn(
        "previous_rate",
        lag("rate").over(currency_window)
    )
    .withColumn(
        "daily_change_pct",
        round(
            ((col("rate") - col("previous_rate")) / col("previous_rate")) * 100,
            4
        )
    )
    .orderBy("quote", "date")
)

display(gold_currency_df)

# COMMAND ----------

from pyspark.sql.functions import lag

gold_currency_df = (
    gold_currency_df
    .withColumn(
        "rate_7_days_ago",
        lag("rate", 7).over(currency_window)
    )
    .withColumn(
        "change_7d_pct",
        round(
            ((col("rate") - col("rate_7_days_ago")) / col("rate_7_days_ago")) * 100,
            4
        )
    )
)

display(gold_currency_df)

# COMMAND ----------

gold_currency_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_currency_analysis")

# COMMAND ----------

gold_saved_df = spark.table("gold_currency_analysis")

print("Gold table rows:", gold_saved_df.count())

display(gold_saved_df)

# COMMAND ----------

from pyspark.sql.functions import max, min, avg

currency_summary_df = (
    gold_saved_df
    .groupBy("quote")
    .agg(
        round(avg("rate"), 4).alias("average_rate"),
        round(min("rate"), 4).alias("minimum_rate"),
        round(max("rate"), 4).alias("maximum_rate"),
        round(avg("daily_change_pct"), 4).alias("average_daily_change_pct"),
        round(avg("change_7d_pct"), 4).alias("average_7d_change_pct")
    )
    .orderBy("quote")
)

display(currency_summary_df)

# COMMAND ----------

currency_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_currency_summary")

# COMMAND ----------

from pyspark.sql.functions import min, max

gold_saved_df.select(
    min("date").alias("first_date"),
    max("date").alias("latest_date")
).show()

# COMMAND ----------

gold_saved_df.groupBy("quote") \
    .count() \
    .orderBy("quote") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

latest_window = Window.partitionBy("quote").orderBy(col("date").desc())

latest_rates_df = (
    gold_saved_df
    .withColumn("row_number", row_number().over(latest_window))
    .filter(col("row_number") == 1)
    .drop("row_number")
    .orderBy("quote")
)

display(latest_rates_df)

# COMMAND ----------

latest_rates_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_latest_exchange_rates")

# COMMAND ----------

daily_movers_df = (
    spark.table("gold_latest_exchange_rates")
    .select(
        "quote",
        "date",
        "rate",
        "daily_change_pct",
        "change_7d_pct"
    )
    .orderBy(col("daily_change_pct").desc())
)

display(daily_movers_df)

# COMMAND ----------

daily_movers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_movers")

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

historical_currency_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_exchange_rates")

# COMMAND ----------

bronze_df = spark.table("bronze_exchange_rates")

print("Bronze table rows:", bronze_df.count())

display(bronze_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     quote,
# MAGIC     rate,
# MAGIC     daily_change_pct,
# MAGIC     change_7d_pct
# MAGIC FROM gold_currency_analysis
# MAGIC ORDER BY date, quote;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     quote,
# MAGIC     daily_change_pct
# MAGIC FROM gold_currency_analysis
# MAGIC WHERE daily_change_pct IS NOT NULL
# MAGIC ORDER BY date, quote;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     quote,
# MAGIC     rate
# MAGIC FROM gold_latest_exchange_rates
# MAGIC ORDER BY rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     quote,
# MAGIC     daily_change_pct
# MAGIC FROM gold_latest_exchange_rates
# MAGIC WHERE daily_change_pct IS NOT NULL
# MAGIC ORDER BY daily_change_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     quote,
# MAGIC     change_7d_pct
# MAGIC FROM gold_latest_exchange_rates
# MAGIC WHERE change_7d_pct IS NOT NULL
# MAGIC ORDER BY change_7d_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     quote,
# MAGIC     average_rate,
# MAGIC     minimum_rate,
# MAGIC     maximum_rate,
# MAGIC     average_daily_change_pct,
# MAGIC     average_7d_change_pct
# MAGIC FROM gold_currency_summary
# MAGIC ORDER BY quote;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MAX(date) AS latest_data_date
# MAGIC FROM gold_currency_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records
# MAGIC FROM gold_currency_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT quote) AS number_of_currencies
# MAGIC FROM gold_currency_analysis;

# COMMAND ----------

postgres_host = "ep-sparkling-art-asbs1tli.c-4.eu-central-1.aws.neon.tech"
postgres_database = "neondb"
postgres_user = "neondb_owner"
postgres_password = "Ydaq2M2aY!3B9GC"

jdbc_url = (
    f"jdbc:postgresql://{postgres_host}:5432/{postgres_database}"
    "?sslmode=require"
)

connection_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

print("PostgreSQL connection settings created")

# COMMAND ----------

postgres_host = "YOUR_POSTGRES_HOST"
postgres_database = "YOUR_DATABASE_NAME"
postgres_user = "YOUR_USERNAME"
postgres_password = "YOUR_PASSWORD"

jdbc_url = (
    f"jdbc:postgresql://{postgres_host}:5432/{postgres_database}"
    "?sslmode=require"
)

connection_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

print("JDBC url updated")

# COMMAND ----------

test_query = "(SELECT 1 AS connection_test) AS test_table"

test_df = spark.read.jdbc(
    url=jdbc_url,
    table=test_query,
    properties=connection_properties
)

display(test_df)

# COMMAND ----------

gold_saved_df = spark.table("gold_currency_analysis")

print("Gold table rows:", gold_saved_df.count())

# COMMAND ----------

gold_saved_df.write \
    .format("postgresql") \
    .mode("overwrite") \
    .option("host", postgres_host) \
    .option("port", "5432") \
    .option("database", postgres_database) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("dbtable", "gold_currency_analysis") \
    .save()

print("gold_currency_analysis loaded into PostgreSQL")

# COMMAND ----------

postgres_gold_df = (
    spark.read
    .format("postgresql")
    .option("host", postgres_host)
    .option("port", "5432")
    .option("database", postgres_database)
    .option("user", postgres_user)
    .option("password", postgres_password)
    .option("dbtable", "gold_currency_analysis")
    .load()
)

print("PostgreSQL table rows:", postgres_gold_df.count())

display(postgres_gold_df)

# COMMAND ----------

tables_to_load = [
    "gold_currency_summary",
    "gold_latest_exchange_rates",
    "gold_daily_movers"
]

for table_name in tables_to_load:
    df = spark.table(table_name)

    (
        df.write
        .format("postgresql")
        .mode("overwrite")
        .option("host", postgres_host)
        .option("port", "5432")
        .option("database", postgres_database)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("dbtable", table_name)
        .save()
    )

    print(f"{table_name} loaded into PostgreSQL")

# COMMAND ----------

postgres_tables = [
    "gold_currency_analysis",
    "gold_currency_summary",
    "gold_latest_exchange_rates",
    "gold_daily_movers"
]

for table_name in postgres_tables:
    df = (
        spark.read
        .format("postgresql")
        .option("host", postgres_host)
        .option("port", "5432")
        .option("database", postgres_database)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("dbtable", table_name)
        .load()
    )

    print(table_name, ":", df.count(), "rows")

# COMMAND ----------

postgres_gold_df = (
    spark.read
    .format("postgresql")
    .option("host", postgres_host)
    .option("port", "5432")
    .option("database", postgres_database)
    .option("user", postgres_user)
    .option("password", postgres_password)
    .option("dbtable", "gold_currency_analysis")
    .load()
)

postgres_gold_df.groupBy("date") \
    .count() \
    .orderBy("date", ascending=False) \
    .show(5)

# COMMAND ----------

total_postgres_rows = postgres_gold_df.count()

unique_postgres_rows = postgres_gold_df.select(
    "date", "base", "quote"
).distinct().count()

print("Total PostgreSQL rows:", total_postgres_rows)
print("Unique PostgreSQL records:", unique_postgres_rows)
print("Duplicate PostgreSQL rows:", total_postgres_rows - unique_postgres_rows)

# COMMAND ----------

