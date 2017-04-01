# !/usr/bin/env python
import sys
import re
from pyspark import SparkContext, SparkConf
def extract_user(line_user):
    return (re.sub('^(.*user )', "", line_user))

if __name__ == "__main__":
conf = SparkConf().setAppName("log_analyzer").setMaster("local")
sc = SparkContext(conf=conf)
if __name__=="__main __":

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
    user_iliad = sc.textFile(input_text_file_1). \
        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
        filter(lambda x: 'Starting session'.lower()). \
        filter(lambda x: 'achille' in x).count()

    user_odyssey = sc.textFile(input_text_file_2). \
        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
        filter(lambda x: 'Starting session'.lower()). \
        filter(lambda x: 'achille' in x).count()
    print '+ iliad:', user_iliad
    print '+ odysse:',user_odyssey

elif (question_number=="-q3"):
        print ("* Q3: unique user names")
        unique_user_iliad= sc.textFile(input_text_file_1).\
                           map(lambda x:x.replace(',',' ').replace('.',' ').lower()).\
                           filter(lambda x: 'Starting session'.lower() in x).\
                           map(lambda x: (extract_user(x))).distinct().collect()
        unique_user_odysse = sc.textFile(input_text_file_2). \
                             map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                             filter(lambda x: 'Starting session'.lower() in x). \
                             map(lambda x: (extract_user(x))).distinct().collect()

        print '+ iliad:', unique_user_iliad
        print '+ odysse:', unique_user_odysse



elif (question_number == "-q4"):
    print ("* Q4: sessions per user")
    unique_user_iliad = sc.textFile(input_text_file_1). \
                        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                        filter(lambda x: 'Starting session'.lower() in x). \
                        map(lambda x: (extract_user(x), 1)). \
                        reduceByKey(lambda x, y: x + y).collect()
    unique_user_odysse = sc.textFile(input_text_file_2). \
                        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                        filter(lambda x: 'Starting session'.lower() in x). \
                        map(lambda x: (extract_user(x), 1)). \
                        reduceByKey(lambda x, y: x + y).collect()

    print '+ iliad:', unique_user_iliad
    print '+ odysse:', unique_user_odysse

elif (question_number == "-q5"):
    print (" * Q5: number of errors")
    unique_user_iliad = sc.textFile(input_text_file_1). \
                        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                        filter(lambda x: 'error'.lower() in x).count()
    unique_user_odyssey = sc.textFile(input_text_file_2). \
                          map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                          filter(lambda x: 'error'.lower() in x).count()
    print '+ iliad:', unique_user_iliad
    print '+ odyssey:', unique_user_odyssey

elif (question_number == "-q6"):
    print (" * Q6: 5 most frequent error messages")
elif (question_number == "-q7"):
    print (" * Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")
elif (question_number == "-q8"):
    print (" * Q8: users who started a session on exactly one host, with host name.")
elif (question_number == "-q9"):
    print (" ---------")






