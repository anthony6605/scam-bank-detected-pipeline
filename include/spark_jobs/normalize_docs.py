import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, trim
def main(in_path:str, out_path:str):
    spark = (
        SparkSession.builder
        .appName("normalize_docs")
        .master("local[*]")
        .getOrCreate()
    )
    df  = spark.read.json(in_path)
    df_cleaned = (
        df.withColumn("text", trim(regexp_replace(col("text"), r"\s+", " ")))
          .withColumn("text", trim(col("text")))
          .filter((col("status_code") >= 200) & (col("status_code") < 300))
          .filter(col("text").isNotNull())
          .filter(length(col("text")) >= 200)
          .dropDuplicates(["doc_id", "content_hash"])
    )
    df_cleaned.write.mode("overwrite").parquet(out_path)

    print("Input rows:", df.count())
    print("Output rows:", df_cleaned.count())
    spark.stop()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    args = ap.parse_args()
    main(args.in_path, args.out_path)