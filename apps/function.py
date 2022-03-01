from pyspark.sql import SparkSession, SQLContext
import mysql.connector
from mysql.connector import errorcode
import requests
import csv

def init_spark():
  spark = SparkSession \
    .builder\
    .appName("titanic-app")\
    .config("spark.jars", "/opt/spark-apps/mysql-connector-java-8.0.28.jar")\
    .getOrCreate()
  sc = spark.sparkContext
  sqlc = SQLContext(sc)
  return spark, sc, sqlc

def get_file():
  url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
  response = requests.get(url)

  with open('/opt/spark-data/titanic.csv', 'w') as f:
    writer = csv.writer(f)
    [writer.writerow(line.decode('utf-8').split(',')) for line in response.iter_lines()]

def prep_table():

  TABLES = {}
  TABLES['titanic_stats'] = (
      "CREATE TABLE `titanic_stats` ("
      "  `desc` char(255) NOT NULL,"
      "  `prcnt` decimal(4,2) NOT NULL"
      ") ENGINE=InnoDB")

  for table_name in TABLES:
    table_description = TABLES[table_name]
    
    try:
        cnx = mysql.connector.connect(user='root', password='iamr00t', host='demo-database', database='titanic_db')
        cursor = cnx.cursor()

        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err.msg)
    else:
        cnx.close()
        print("OK")  

def main():
    
    get_file()
    prep_table()

    spark, sc, sqlc = init_spark()
    data = spark.read.load("/opt/spark-data/titanic.csv", format = "csv", sep=",", header="true")
    
    survived_children = data[(data["Survived"] == 1) & (data["Age"] < 18)]
    survived_children_percent = survived_children.count()/data.count() * 100

    survived_adults = data[(data["Survived"] == 1) & (data["Age"].between(18, 40))]
    survived_adults_percent = survived_adults.count()/data.count() * 100

    survived_male_adults = data[(data["Survived"] == 1) & (data["Age"].between(18, 40)) & (data["Sex"] == 'male')]
    survived_male_adults_percent = survived_male_adults.count()/data.count() * 100

    survived_female_adults = data[(data["Survived"] == 1) & (data["Age"].between(18, 40)) & (data["Sex"] == 'female')]
    survived_female_adults_percent = survived_female_adults.count()/data.count() * 100

    survived_elderly = data[(data["Survived"] == 1) & (data["Age"] > 40)]
    survived_elderly_percent = survived_elderly.count()/data.count() * 100

    survived_male_elderly = data[(data["Survived"] == 1) & (data["Age"] > 40) & (data["Sex"] == 'male')]
    survived_male_elderly_percent = survived_male_elderly.count()/data.count() * 100

    survived_female_elderly = data[(data["Survived"] == 1) & (data["Age"] > 40) & (data["Sex"] == 'female')]
    survived_female_elderly_percent = survived_female_elderly.count()/data.count() * 100

    #Divide the number of survivors for each class by [total number of survivors]
    survived_male_class_one = data[(data["Pclass"] == 1) & (data["Survived"] == 1) & (data["Sex"] == 'male')]
    survived_male_class_one_percent = survived_male_class_one.count()/data.count() * 100

    survived_male_class_two = data[(data["Pclass"] == 2) & (data["Survived"] == 1) & (data["Sex"] == 'male')]
    survived_male_class_two_percent = survived_male_class_two.count()/data.count() * 100
  
    survived_male_class_three = data[(data["Pclass"] == 3) & (data["Survived"] == 1) & (data["Sex"] == 'male')]
    survived_male_class_three_percent = survived_male_class_three.count()/data.count() * 100

    survived_female_class_one = data[(data["Pclass"] == 1) & (data["Survived"] == 1) & (data["Sex"] == 'female')]
    survived_female_class_one_percent = survived_female_class_one.count()/data.count() * 100

    survived_female_class_two = data[(data["Pclass"] == 2) & (data["Survived"] == 1) & (data["Sex"] == 'female')]
    survived_female_class_two_percent = survived_female_class_two.count()/data.count() * 100
  
    survived_female_class_three = data[(data["Pclass"] == 3) & (data["Survived"] == 1) & (data["Sex"] == 'female')]
    survived_female_class_three_percent = survived_female_class_three.count()/data.count() * 100

    data = [("survived_children", round(survived_children_percent, 2)),
            ("survived_adults", round(survived_adults_percent, 2)),
            ("survived_adults_males", round(survived_male_adults_percent, 2)),
            ("survived_adults_females", round(survived_female_adults_percent, 2)),
            ("survived_elderly", round(survived_elderly_percent, 2)),
            ("survived_elderly_male", round(survived_male_elderly_percent, 2)),
            ("survived_elderly_female", round(survived_female_elderly_percent, 2)),
            ("survival_rate_class_one_male", round(survived_male_class_one_percent, 2)),
            ("survival_rate_class_two_male", round(survived_male_class_two_percent, 2)),
            ("survival_rate_class_three_male", round(survived_male_class_three_percent, 2)),
            ("survival_rate_class_one_female", round(survived_female_class_one_percent, 2)),
            ("survival_rate_class_two_female", round(survived_female_class_two_percent, 2)),
            ("survival_rate_class_three_female", round(survived_female_class_three_percent, 2))
    ]
    
    df = sqlc.createDataFrame(data)
    print(df.show())
    
    df = sc.parallelize(data).toDF(['desc', 'prcnt'])
    df.write.format('jdbc').options(
          url='jdbc:mysql://demo-database:3306/titanic_db',
          driver='com.mysql.cj.jdbc.Driver',
          dbtable='titanic_stats',
          user='root',
          password='iamr00t').mode('append').save()

if __name__ == '__main__':
  main()
