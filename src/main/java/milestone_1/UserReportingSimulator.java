package milestone_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static  org.apache.spark.sql.functions.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;


public class UserReportingSimulator {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // defualt port
        props.put("acks", "all"); //  https://kafka.apache.org/documentation/
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // built in serilaizable data for java object
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//        SparkConf conf = new SparkConf().setAppName("Main").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        SparkSession sparkSQL = SparkSession.builder()
//                .appName("SparkSQL")
//                .master("local[*]")
//                .getOrCreate();

//        data.forEach(e -> System.out.println(e));

        Scanner sc = new Scanner(new FileReader("src/main/resources//books_dataset/Users.csv"));

        int i =0;
        sc.nextLine();
        while (sc.hasNextLine()) {
            String data = sc.nextLine();
            System.out.println(data);
            i++;
            if(i == 1000) {
                break;
            }
            producer.send(new ProducerRecord<>("user",data));
        }
        sc.close();
        producer.close();

//        Dataset<Row> bookRDD = sparkSQL.read().option("header", true).csv("src/main/resources//books_dataset/Books.csv");
//        Dataset<Row> ratingRDD = sparkSQL.read().option("header",true).csv("src/main/resources//books_dataset/Ratings.csv");
//        Dataset<Row> userRDD= sparkSQL.read().option("header",true).csv("src/main/resources//books_dataset/Users.csv");

//        Dataset<Row> isbnRDD = bookRDD.select(col("ISBN"));
//        Dataset<Row> titleRDD = bookRDD.select(col("Book_Title"));
//        Dataset<Row> yearRDD = bookRDD.select(col("Yeah_Published").alias("Published Year"));
//        yearRDD.show();

//        JavaRDD<Row> bookRatingRDD = ratingRDD.join(titleRDD).join(yearRDD).toJavaRDD();
//
//
//        AtomicInteger i = new AtomicInteger();
//
//        bookRatingRDD.foreach(e -> {
//            String data = e.getString(e.fieldIndex("Book_Title"));
//            System.out.println(data);
//            i.getAndIncrement();
//            if(i.get() == 2000) System.exit(1000);
//        });

    }



}
