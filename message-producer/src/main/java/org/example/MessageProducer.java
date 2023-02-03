package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class MessageProducer {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
                .builder()
                .appName("MessageProducer")
                .master("local[*]")
                .getOrCreate();

        StructType messageSchema = new StructType()
                .add("service_id", "integer")
                .add("vehicle_vin", "string")
                .add("itemid", "string")
                .add("item_description", "string")
                .add("quantity", "integer")
                .add("rate", "integer");

        Dataset<Row> messageStream = spark
                .readStream()
                .schema(messageSchema)
                .json("d:/message-content");

        StreamingQuery query = messageStream
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "topic-one")
                .start();

        query.awaitTermination();
    }
}
