from pyspark.sql import SparkSession

def main():
    try:
        # TODO log spark configurations.
        spark
    except NameError:
        # TODO tests
        spark = (SparkSession.builder
                            .appName("SparkTest") # Set app name
                            .master("local[2]") # Run locally with 2 cores
                            .config("spark.driver.memory", "4g")
                            .config("spark.executor.memory", "3g")
                            .getOrCreate())

    print('@@@ Configurations @@@')
    print(spark.sparkContext._conf.getAll())


if __name__ == "__main__":
    main()