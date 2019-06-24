import sys

from __future__ import print_function
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("word_count").getOrCreate()
    spark_context = spark.sparkContent
    input_file = "lorem.txt"

    text_file_rdd = sc.textFile(input_file)
    print(text_file_rdd.collect())

    words_rdd = text_file_rdd.flatMap(lambda line: line.split(" "))
    print(words_rdd.collect())

    pairs_rdd = words_rdd.map(lambda word: (word, 1))
    print(pairs_rdd.collect())

    frequencies_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)
    print(frequencies_rdd.collect())

    spark.stop()