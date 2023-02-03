package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

public class MessageTransformer {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("MessageTransformer")
                .getOrCreate();

        // Define the schema for the input data
        StructType schema = new StructType()
                .add("service_id", "integer")
                .add("vehicle_vin", "string")
                .add("itemid", "string")
                .add("item_description", "string")
                .add("quantity", "integer")
                .add("rate", "integer");

        // Read input data as a Dataset
        Dataset<Row> inputData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic-one")
                .load();

        // Add the "price" column to the input data
        Dataset<Row> transformedData = inputData
                .withColumn("price", inputData.col("quantity").multiply(inputData.col("rate")));

        // Write the transformed data to the output topic
        transformedData
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "topic-two")
                .option("checkpointLocation", "d:/message-content")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();
    }
}
