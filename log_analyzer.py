# !/usr/bin/env python
import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("log_analyzer").setMaster("local")
sc = SparkContext(conf=conf)

question_number = sys.argv[1]
input_text_file_1 = sys.argv[2]
input_text_file_2 = sys.argv[3]

if (question_number == "-q1"):
    print (" *Q1: line counts")
    counts_iliad = sc.textFile(input_text_file_1). \
        count()
    counts_odyssey = sc.textFile(input_text_file_2). \
        count()
    print '+ iliad:', counts_iliad
    print '+ odyssey:', counts_odyssey


elif (question_number == "-q2"):
    print (" Q2: sessions of user 'achille'")
elif (question_number == "-q3"):
    print ("* Q3: unique user names")
elif (question_number == "-q4"):
    print (" * Q4: sessions per user")
elif (question_number == "-q5"):
    print (" * Q5: number of errors")
elif (question_number == "-q6"):
    print (" * Q6: 5 most frequent error messages")
elif (question_number == "-q7"):
    print (" * Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")
elif (question_number == "-q8"):
    print (" * Q8: users who started a session on exactly one host, with host name.")
elif (question_number == "-q9"):
    print (" ---------")






