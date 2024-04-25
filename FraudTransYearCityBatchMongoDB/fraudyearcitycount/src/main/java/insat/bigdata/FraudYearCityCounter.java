package insat.bigdata;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.bson.Document;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FraudYearCityCounter {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "fraud year City counter");
        job.setJarByClass(FraudYearCityCounter.class);
        job.setMapperClass(YearCityFraudMappper.class);
        job.setReducerClass(FraudCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        


        
        
            if(job.waitForCompletion(true)){
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(args[1]));
            for (int i = 0; i < status.length; i++) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                List<Document> docs = new ArrayList<>();
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    String[] dateAndCity = fields[0].split(",");
                   

                   
                    String date = dateAndCity[0];
                    String city = dateAndCity[1];
                    int count = Integer.parseInt(fields[1]);
                    
                    Document doc = new Document("date", date)
                        
                        .append("city", city)
                        .append("count", count);
                    
                    docs.add(doc);
                    
                }
                if(!docs.isEmpty()){
                    String uri = "mongodb+srv://wiembenmlouka:DtdUWLKUIBolpr2h@cluster0.qmhumny.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
                    
                    ConnectionString mongoURI = new ConnectionString(uri);
                    MongoClientSettings settings = MongoClientSettings.builder()
                            .applyConnectionString(mongoURI)
                            .build();
                    MongoClient mongoClient = MongoClients.create(settings);
                    MongoDatabase database = mongoClient.getDatabase("Fraud_Transactions_db");
                    MongoCollection<Document> collection = database.getCollection("fraud_date_city_counts");
                    
                    collection.insertMany(docs);
                    mongoClient.close();
                    
                }
            }
            System.exit(0);
        } else {
            System.exit(1);
        }
        System.exit(1);
       
    }

    
}
