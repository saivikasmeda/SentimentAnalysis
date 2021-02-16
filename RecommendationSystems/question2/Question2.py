#!/usr/bin/env python
# coding: utf-8

# In[1]:


from math import sqrt
import numpy as np
from numpy import array
import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans


# In[2]:


#setting the string variables
app_name = 'Movie-User Matrix'
master = 'local'

#setting the data file for item-user matrix and num of clusters
itemusermatdata_path = '../Input/itemusermat.data'
num_cluster = 10

#setting the data file for movies
moviesdata_path = '../Input/movies.data'


# In[3]:


spark = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=spark)


# In[4]:


def mapper_movies_data(line):
    data = line.split('::')
    data[0] = float(data[0])
    data[2] = data[2].split('|')
    genre = ''
    for i in data[2]:
        genre += i + ', '
    genre = genre[:-2]
    data[2] = genre
    return (data[0], data[1:])

def mapper_itemuser_data(line):
    data = line.split(' ')
    n = len(data)

    for i in range(n):
        data[i] = float(data[i])
    return data


# In[5]:


itemuser_data_rdd = sc.textFile(itemusermatdata_path)
movies_data_rdd = sc.textFile(moviesdata_path)

parse_itemuser_data_rdd = itemuser_data_rdd.map(mapper_itemuser_data)
parse_movies_Data_rdd = movies_data_rdd.map(mapper_movies_data)




kmeansModel = KMeans.train(parse_itemuser_data_rdd, num_cluster, maxIterations=500)
predicted_data = kmeansModel.predict(parse_itemuser_data_rdd)


# In[ ]:


def combine_rdds_mapper(x):
    temp = np.append(x[0], x[1])
    return (temp[0], temp[-1])

itemuser_prediction_rdd = parse_itemuser_data_rdd.zip(predicted_data).map(combine_rdds_mapper)


# In[ ]:


combined_data_rdd = itemuser_prediction_rdd.join(parse_movies_Data_rdd).map(lambda line: (line[0], line[1][0], line[1][1]))


# In[ ]:


final_data_rdd = combined_data_rdd.groupBy(lambda line: line[1]).sortByKey(True).map(
    lambda line: (line[0], list(line[1])[1:6]))
with open('MovieDetails.txt', 'w') as f:
    for i in final_data_rdd.take(10):
        f.write('Cluster ' + str(int(i[0]) + 1) + '\n')
        for j in i[1]:
            f.write(str(int(j[0])) + ' ' + j[2][0] + ' ' + j[2][1] + '\n')
        f.write('\n\n\n')
f.close()

