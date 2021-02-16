
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import Rating
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

app_name = 'Accuracy ALS'
master = 'local'

sc = SparkContext(master=master, appName=app_name)
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = sc.textFile("../Input/ratings.data")
ratings_data = data.map(lambda l: l.split('::')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
df = ratings_data.toDF()

#defining model properties
als = ALS(maxIter=10, regParam=0.5, userCol="user", itemCol = "product", ratingCol = "rating", coldStartStrategy = "drop")

#Splitting data for training and testing
trainData, testData = df.randomSplit([0.7, 0.3])

#Training the Model
alsModel = als.fit(trainData)

#Testing the Model
prediction = alsModel.transform(testData)
prediction.show(5)
evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",  predictionCol="prediction")
acc = evaluator.evaluate(prediction)

#Printing accuracy
print("Accuracy: ", acc)
