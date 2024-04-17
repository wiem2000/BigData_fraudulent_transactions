package insat.bigdata;

import org.apache.spark.sql.*;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import java.util.Arrays;

import com.opencsv.CSVParser;

public class StreamWithKafka {
    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
        if (args.length < 3) {
            System.err.println("Usage: StreamWithKafka <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("CategoryFraudCountWithKafka")
                .master("local[*]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();

        // Assuming your Kafka messages are key-value pairs, adjust this as needed
        df.selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .flatMap(
                    (String x) -> {
                        // Your logic here to parse CSV from Kafka message
                        CSVParser parser = new CSVParser();
                        String[] fields = parser.parseLine(x);
                        if (fields != null && fields.length >= 23 && !fields[1].equals("trans_date_trans_time") && fields[22].equals("1")) {
                            return Arrays.asList(fields[4]).iterator();
                        } else {
                            return Arrays.<String>asList().iterator();  
                        }
                    },
                    Encoders.STRING())
                .toDF("category")
                .groupBy("category")
                .count()
                .writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start()
                .awaitTermination();
    }
}
