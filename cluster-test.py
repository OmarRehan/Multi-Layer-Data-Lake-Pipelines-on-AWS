from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('blabla') \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.format('csv')\
        .option('header','true')\
        .load('s3a://sparkify-test-2020/L_WORLD_AREA_CODES/')

    df.show()

    df.write.format('csv').option('header','true').save('hdfs:////FLIGHTS_DL/L_WORLD_AREA_CODES')

    spark.stop()
