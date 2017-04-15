# !/usr/bin/env python
import sys
from pyspark import SparkContext, SparkConf
import re

def extract_user(line_user):
    return (re.sub('^(.*user )', "", line_user))


def format_tuple(tuple_id):
    tuple_value = tuple_id[1]
    format_usrer = "user-" + str(tuple_value)
    return (tuple_id[0], format_usrer)


def findErrorLine(line):
    result = re.findall('(?i)error', line)
    if (len(result) > 0):
        return True
    else:
        return False


def remove_date_format(line):
    return (re.sub('[A-Za-z]{3}\s+\d{1,2}\s(?:\d{1,2}:){2}\d{1,2}', " ", line))


def zip_function(value, index):
    return (value, index)


def replace_fun(line, tup):
    flag = False
    m = ""
    new_line = line
    for x in tup:

        if (x[0] in line):
            m = line.replace(x[0], x[1])

            flag = True

    if (flag == False):
        return new_line
    else:
        return m


if __name__ == "__main__":
    conf = SparkConf().setAppName("wordcount").setMaster("local")
    sc = SparkContext(conf=conf)
    question_number = sys.argv[1]
    iliad = sys.argv[2]
    odyssey = sys.argv[3]
    if (question_number == "-q1"):
        print (" *Q1: line counts")
        counts_iliad = sc.textFile(iliad). \
            count()
        counts_odyssey = sc.textFile(odyssey). \
            count()
        print '+ iliad:', counts_iliad
        print '+ odyssey:', counts_odyssey



    elif (question_number == "-q2"):
        # we could use regex ("Starting session\s+\d\s+ of user\[A-Za-z]")
        print ("* Q2: sessions of user 'achille'")
        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'Starting session'.lower() in x). \
            filter(lambda x: 'achille' in x).count()
        print '+ iliad:', unique_user_iliad

        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'Starting session'.lower() in x). \
            filter(lambda x: 'achille' in x).count()
        print '+ odyssey:', unique_user_odyssey

    elif (question_number == "-q3"):
        print ("* Q3: unique user names")
        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'Starting session'.lower() in x). \
            map(lambda x: (extract_user(x))).distinct().collect()
        print '+ iliad:', unique_user_iliad

        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'Starting session'.lower() in x). \
            map(lambda x: (extract_user(x))).distinct().collect()
        print '+ odyssey:', unique_user_odyssey

    elif (question_number == "-q4"):
        print ("* Q4: sessions per user")
        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'Starting session'.lower() in x). \
            map(lambda x: (extract_user(x), 1)). \
            reduceByKey(lambda x, y: x + y).collect()
        print '+ iliad:', unique_user_iliad

        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'Starting session'.lower() in x). \
            map(lambda x: (extract_user(x), 1)). \
            reduceByKey(lambda x, y: x + y).collect()
        print '+ odyssey:', unique_user_odyssey

    elif (question_number == "-q5"):
        print ("* Q5: number of errors")
        # we can use regex
        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'error'.lower() in x).count()
        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', ' ').replace('.', ' ').lower()). \
            filter(lambda x: 'error'.lower() in x).count()
        print '+ iliad:', unique_user_iliad
        print '+ odyssey:', unique_user_odyssey

    elif (question_number == "-q6"):
        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: remove_date_format(x)). \
            filter(lambda x: findErrorLine(x) == True). \
            map(lambda x: (x, 1)). \
            reduceByKey(lambda x, y: x + y)
        result_iliad = unique_user_iliad.takeOrdered(5, key=lambda x: -x[1])

        unique_user_odyssey = sc.textFile(odyssey). \
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

        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', '').replace('.', '')). \
            filter(lambda x: 'Starting Session' in x). \
            map(lambda x: (extract_user(x))).distinct()
        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', '').replace('.', '')). \
            filter(lambda x: 'Starting Session' in x). \
            map(lambda x: (extract_user(x))).distinct()
        result = unique_user_iliad.intersection(unique_user_odyssey).collect()
        print ("* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")
        print '+:', result

    elif (question_number == "-q8"):

        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', '').replace('.', '')). \
            filter(lambda x: 'Starting Session' in x). \
            map(lambda x: (extract_user(x))). \
            map(lambda x: (x, "iliad"))

        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', '').replace('.', '')). \
            filter(lambda x: 'Starting Session' in x). \
            map(lambda x: (extract_user(x))). \
            map(lambda x: (x, "odyssey"))
        re1 = unique_user_iliad.subtractByKey(unique_user_odyssey)
        re2 = unique_user_odyssey.subtractByKey(unique_user_iliad)
        rdd1 = re1.distinct()
        rdd = re2.distinct()
        print ("* Q8: users who started a session on exactly one host, with host name.")
        print '+:', rdd.union(rdd1).collect()


    elif (question_number == "-q9"):

        unique_user_iliad = sc.textFile(iliad). \
            map(lambda x: x.replace(',', '').replace('.', '')). \
            filter(lambda x: 'Starting Session' in x). \
            map(lambda x: (extract_user(x))).distinct(). \
            map(lambda x: (x, 0)).sortByKey(). \
            map(lambda x: x[0]).zipWithIndex()
        result_iliad = unique_user_iliad.map(lambda x: format_tuple(x)).collect()
        new_file_iliad = sc.textFile(iliad). \
            map(lambda x: replace_fun(x, result_iliad))

        unique_user_odyssey = sc.textFile(odyssey). \
            map(lambda x: x.replace(',', '').replace('.', '')). \
            filter(lambda x: 'Starting Session' in x). \
            map(lambda x: (extract_user(x))).distinct(). \
            map(lambda x: (x, 0)).sortByKey(). \
            map(lambda x: x[0]).zipWithIndex()
        result_odyssey = unique_user_odyssey.map(lambda x: format_tuple(x)).collect()
        new_file_odyssey = sc.textFile(odyssey). \
            map(lambda x: replace_fun(x, result_odyssey))

        print 'User name mapping:', result_iliad
        print'Anonymized files: iliad-anonymized-10'
        new_file_iliad.saveAsTextFile("iliad-anonymized10")

        print 'User name mapping:', result_odyssey
        print'Anonymized files: odyssey-anonymized-10'
        new_file_odyssey.saveAsTextFile("odyssey-anonymized10")

















