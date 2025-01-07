import logging
import uuid

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.weather (            
        id text PRIMARY KEY,
        city_name text,
        wind_speed FLOAT,            
        latitude FLOAT,
        longtitude FLOAT,
        time_of_record INT,
        surnise_time INT,
        sunset_time INT,
        temperature FLOAT,                                               
        humidity FLOAT 
                    );
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    
    id = kwargs.get('id')
    city_name=kwargs.get('city_name')
    wind_speed = kwargs.get('wind_speed')
    latitude = kwargs.get('latitude')
    longtitude = kwargs.get('longtitude')
    time_of_record = kwargs.get('time_of_record')
    sunrise_time =kwargs.get('sunrise_time')
    sunset_time = kwargs.get('sunset_time')
    temperature = kwargs.get('temperature')
    humidity = kwargs.get('humidity')

    try:
        session.execute("""
            INSERT INTO spark_streams.weather(id, city_name,wind_speed, latitude,longtitude, 
                time_of_record,sunrise_time ,sunset_time,temperature, temp_max)
                VALUES ( %s,%s, %s, %s, %s, %s, %s, %s,%s, %s)
        """, (id, city_name,wind_speed,latitude, longtitude,
              time_of_record,sunrise_time,sunset_time, temperature, humidity))
        logging.info(f"Data inserted for {id} {wind_speed}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')




def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12-3.5.1,""kafka-clients-3.0.0," "spark-cassandra-connector-assembly_2.12-3.5.1,"
                    "org.apache.spark:spark-streaming-kafka-0-10_2.12-3.5.4,") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'project1') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("city_name", StringType(), False),
        StructField("wind_speed", StringType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longtitude", FloatType(), False),
        StructField("time_of_record", IntegerType(), False),
        StructField("surnise_time", IntegerType(), False),
        StructField("sunset_time", IntegerType(), False),
        StructField("temperature", FloatType(), False),
        StructField("humidity", FloatType(), False)
        ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'weather')
                               .start())

            streaming_query.awaitTermination()