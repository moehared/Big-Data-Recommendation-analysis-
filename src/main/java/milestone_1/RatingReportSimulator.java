package milestone_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;

public class RatingReportSimulator {
    public static void main(String[] args) throws FileNotFoundException {

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

        Scanner sc = new Scanner(new FileReader("src/main/resources//books_dataset/Ratings.csv"));

        int i =0;
        sc.nextLine();
        while (sc.hasNextLine()) {
            String data = sc.nextLine();
            System.out.println(data);
            i++;
            if(i == 1000) {
                break;
            }
            producer.send(new ProducerRecord<>("rating",data));
        }
        sc.close();
        producer.close();
    }
}
