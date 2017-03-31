#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("testFunction").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file = sys.argv[1]
output_text_file = sys.argv[2]

counts = sc.textFile(input_text_file).\
         map(lambda line : line.split(" ")).




result.saveAsTextFile(output_text_file)
