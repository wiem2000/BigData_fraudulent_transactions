package insat.bigdata;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import java.util.Arrays;

import com.opencsv.CSVParser;

public class Stream {
    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
       SparkSession spark = SparkSession
                .builder()
                .appName("CategoryFraudCount")
                .master("local[*]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<String> lines = spark
                .readStream()
                .format("socket")
                .option("host", "172.23.105.251") 
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        // Convert each line to a Dataset of CSV rows
        Dataset<Row> rows = lines.flatMap(
        (String x) -> {
            CSVParser parser = new CSVParser();
            String[] fields = parser.parseLine(x);
            if (fields != null && fields.length >= 23 && !fields[1].equals("trans_date_trans_time") && fields[22].equals("1")) {
                return Arrays.asList(fields[4]).iterator();
            } else {
                return Arrays.<String>asList().iterator();  // Explicitly specify the type for empty iterator
            }
        },
        Encoders.STRING()).toDF("category");
       
        // Group by category and count occurrences
        Dataset<Row> categoryCounts = rows.groupBy("category").count();

        // Start running the query that prints the counts to the console
        StreamingQuery query = categoryCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination();
    }
}