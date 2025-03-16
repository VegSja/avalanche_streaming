# docker compose exec -it spark-master spark-submit \
#   --master spark://spark-master:7077 \
#   --deploy-mode client \
#   --conf "spark.cassandra.connection.host=cassandra_db" \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.commons:commons-pool2:2.8.0,org.apache.kafka:kafka-clients:2.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.2 \
#   /opt/spark/spark-jobs/avalanche_processor.py
#
# sleep 10

docker compose exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf "spark.cassandra.connection.host=cassandra_db" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.commons:commons-pool2:2.8.0,org.apache.kafka:kafka-clients:2.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.2 \
  /opt/spark/spark-jobs/weather_processor.py
