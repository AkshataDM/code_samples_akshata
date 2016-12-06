# Databricks notebook source exported at Tue, 15 Nov 2016 14:37:21 UTC
word_rdd = sc.textFile("/FileStore/tables/gfpt6abn1478874921465/dataSet10.txt")

ACCESS_KEY = "access key"
SECRET_KEY = "secret key"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "oxta"
MOUNT_NAME = "/output/Spark5/Run2"

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)


import re
def spaces(pattern):
  return pattern.split(" ")

def remove_punctuation(text):
    return re.sub('[^a-z:1-9:a-z:1-9| ]', '', text.strip().lower())
def encode_ascii(s):
  return s.encode('ascii','ignore')


split_rdd_word = word_rdd.map(remove_punctuation)

split_rdd_word.take(4)

split_rdd = split_rdd_word.filter(lambda x: x != "")

split_rdd = split_rdd.map(encode_ascii)

split_rdd = split_rdd.map(spaces)
split_rdd.first()

## Creating a map where the first element of each RDD element list is the key and the rest of the words are values, stored as a list

pairs = split_rdd.map(lambda x: (x[0], x[1:]))

pairs.collect()

## associating each value in the key,value pair with the same key. 
def remove_punc_keys(s):
  return (re.sub('[^a-z| ]','',s[0].strip()),s[1])


unpack = pairs.flatMap(lambda x: [(i, x[0]) for i in x[1]])
unpack = unpack.map(remove_punc_keys)
unpack.sortByKey().collect()

#removing punctuation from keys

grouped = unpack.groupByKey().mapValues(list)
inverted_index = grouped.map(lambda x: (x[0],list(set(x[1]))))
inverted_index.collect()

inverted_index.collect()

#removing keys that are null - this happens because when we strip the punctuation, spaces somehow are created
inverted_index = inverted_index.filter(lambda x: len(x[0])>0)
inverted_index = inverted_index.sortByKey()

inverted_index.collect()

## sorting by length of list values
extra = inverted_index.sortBy(lambda x : (-len(x[1]),x[0]), True)
extra.collect()

inverted_index.coalesce(1).saveAsTextFile("/mnt/%s/output/Spark5/Run2/Assign5/" % MOUNT_NAME)

extra.coalesce(1).saveAsTextFile("/mnt/%s/output/Spark5/Run2/Assign5/ExtraCredit/" % MOUNT_NAME)
