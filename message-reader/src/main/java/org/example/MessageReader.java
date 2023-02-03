package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class MessageReader {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("MessageReader")
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

        // Show the input data
        inputData.show();

        inputData.awaitTermination();
    }
}
