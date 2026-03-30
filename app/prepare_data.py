from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


# df.write.csv("/index/data", sep = "\t")