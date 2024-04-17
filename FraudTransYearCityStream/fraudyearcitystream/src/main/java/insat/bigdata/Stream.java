package insat.bigdata;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;

import com.opencsv.CSVParser;

import java.util.concurrent.TimeoutException;


public class Stream {
    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
            .builder()
            .appName("FraudDetection")
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
                    String monthYearCity = fields[1].substring(0, 7) + "," + fields[10];
                    return Arrays.asList(monthYearCity).iterator();
                } else {
                    return Arrays.<String>asList().iterator();
                }
            },
            Encoders.STRING()).toDF("monthYearCity");

        // Group by monthYearCity and count occurrences
        Dataset<Row> fraudCounts = rows.groupBy("monthYearCity").count();

        // Start running the query that prints the counts to the console
        StreamingQuery query = fraudCounts.writeStream()
            .outputMode("complete")
            .format("console")
            .trigger(Trigger.ProcessingTime("1 second"))
            .start();

        query.awaitTermination();
    }
}
