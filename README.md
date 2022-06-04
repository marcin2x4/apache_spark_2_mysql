# apache_spark_2_mysql

Slightly modified solution by Marco (https://dev.to/mvillarrealb/creating-a-spark-standalone-cluster-with-docker-and-docker-compose-2021-update-6l4)

# After you git clone...
Run `docker build -t cluster-apache-spark:3.0.2 .`, remember to be in the directory where Dockerfile resides.
Lets bring the cluster to life with `docker-compose up -d` and verify http://localhost:9090 works.

# Deeper dive into python code
`function.py` is made out of few functions which are referenced in `main()`. Data is downloaded from url, transformed in Spark dataframe and written into MySQL.
SQL `CREATE` statement is embedded in `prep_table()` method.

# Run Spark job
Below code will submit the job. Run once logged in into the master node (`docker exec -it apache_spark_2_mysql_spark-master_1 bash`)
Simultaneously, open MySQL instance for querying data (`docker exec -it apache_spark_2_mysql_demo-database_1 bash` / `mysql -uiamuser -piampass`)

`/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--jars /opt/spark-apps/mysql-connector-java-8.0.28.jar \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/function.py`

or

`/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark-apps/mysql-connector-java-8.0.28.jar --driver-memory 1G --executor-memory 1G /opt/spark-apps/function.py`

http://localhost:9090 will also list the job in Running Application section.

# Verify data
Simple `select * from titanic_db.titanic_stats;` will retreive the data.

Once finished wrap up with:

`docker container stop $(docker container ls --all --quiet --filter name=apache_spark_2_mysql-)`

`docker system prune --all --force --volumes`
