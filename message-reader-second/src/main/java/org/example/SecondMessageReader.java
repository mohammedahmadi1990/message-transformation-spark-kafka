package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SecondMessageReader {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("MessageConsumer")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> messageStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic-two")
                .load();

        StreamingQuery query = messageStream
                .writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
