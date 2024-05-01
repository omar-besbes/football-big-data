from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("FootballEventProcessing").getMaster("local").build()
    df = spark.read.csv("/path/to/events.csv", header=True, inferSchema=True)
    # Process data
    df.show()

if __name__ == "__main__":
    main()
