package insat.bigdata;

import org.apache.spark.sql.*;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import java.util.Arrays;

import com.opencsv.CSVParser;

import com.mongodb.client.MongoClients;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

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
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.foreach(row -> {
                        String category = row.getString(0);
                        long count = row.getLong(1);

                        // Create a MongoDB document with the category and count
                        Document document = new Document("category", category)
                                                .append("count", count);

                        // Connect to MongoDB
                        try  {
                            String uri = "mongodb+srv://wiembenmlouka:DtdUWLKUIBolpr2h@cluster0.qmhumny.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
                            ConnectionString mongoURI = new ConnectionString(uri);
                            MongoClientSettings settings = MongoClientSettings.builder()
                                    .applyConnectionString(mongoURI)
                                    .build();
                            MongoClient mongoClient = MongoClients.create(settings);
                            MongoDatabase database = mongoClient.getDatabase("Fraud_Transactions_db");
                            MongoCollection<Document> collection = database.getCollection("fraud_category_counts_stream");
                            
                            // Insert document into MongoDB
                            collection.insertOne(document);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                })
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start()
                .awaitTermination();
       
            }
}
