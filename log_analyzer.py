# !/usr/bin/env python
import sys
import re
from pyspark import SparkContext, SparkConf
def extract_user(line_user):
    return (re.sub('^(.*user )', "", line_user))

def format_tuple(tuple_id):
    tuple_value= tuple_id[1]
    format_usrer= "usrer-"+str(tuple_value)
    return (tuple_id[0],format_usrer)

def findErrorLine(line):
    result= re.findall('(?i)error', line)
    if(len(result)>0):
        return True
    else:
        return False

def remove_date_format(line):
    return (re.sub('[A-Za-z]{3}\s+\d{1,2}\s(?:\d{1,2}:){2}\d{1,2}'," ",line))

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


elif (question_number=="-q6"):
         unique_user_iliad= sc.textFile(input_text_file_1).\
                            map(lambda x : remove_date_format(x)).\
                            filter(lambda x: findErrorLine(x)== True).\
                            map(lambda x: (x,1)).\
                            reduceByKey(lambda x,y: x+y)
         result_iliad = unique_user_iliad.takeOrdered(5, key=lambda x: -x[1])

         unique_user_odyssey = sc.textFile(input_text_file_2). \
                               map(lambda x: remove_date_format(x)). \
                               filter(lambda x: findErrorLine(x) == True). \
                               map(lambda x: (x, 1)). \
                               reduceByKey(lambda x, y: x + y)
        result_odyssey = unique_user_odyssey.takeOrdered(5, key=lambda x: -x[1])

         print '+iliad'
         for x in result_iliad:
             print (x[1], x[0])
         print '+odyssey:'
         for y in result_odyssey:
             print (y[1], y[0])


elif (question_number == "-q7"):
    print ("* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")
    unique_user_iliad = sc.textFile(input_text_file_1). \
                        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                        filter(lambda x: 'Starting session'.lower() in x). \
                        map(lambda x: (extract_user(x))).distinct()
    unique_user_odyssey = sc.textFile(input_text_file_2). \
                          map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                          filter(lambda x: 'Starting session'.lower() in x). \
                          map(lambda x: (extract_user(x))).distinct()
    result = unique_user_iliad.intersection(unique_user_odyssey)
    print '+:', result.collect()

elif (question_number == "-q8"):
    print (" * Q8: users who started a session on exactly one host, with host name.")
    unique_user_iliad = sc.textFile(input_text_file_1). \
                        map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                        filter(lambda x: 'Starting session'.lower() in x). \
                        map(lambda x: (extract_user(x))).\
                        map(lambda x: (x, "iliad"))

    unique_user_odyssey = sc.textFile(input_text_file_2). \
                          map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
                          filter(lambda x: 'Starting session'.lower() in x). \
                          map(lambda x: (extract_user(x))). \
                          map(lambda x: (x, "odyssey"))

    re1 = unique_user_iliad.subtractByKey(unique_user_odyssey)
    re2 = unique_user_odyssey.subtractByKey(unique_user_iliad)
    rdd1 = re1.distinct()
    rdd = re2.distinct()
    print '+:', rdd.union(rdd1).collect()


elif (question_number=="-q9"):
         print ("* Q9: Anonymize the logs.")
         unique_user_iliad= sc.textFile(input_text_file_1).\
                           map(lambda x:x.replace(',',' ').replace('.',' ').lower()).\
                           filter(lambda x: 'Starting session'.lower() in x).\
                           map(lambda x: (extract_user(x))).distinct()
         result = unique_user_iliad.zipWithIndex(). \
                  map(lambda x: format_tuple(x))
         print result.collect()





