package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.json4s.JsonAST;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class UserRecordStream  {
    String url =
            "jdbc:mysql://localhost:3306/userData?user=root;password=Hooyo14#";

    public static void main(String[] args) throws TimeoutException, StreamingQueryException, SQLException, ClassNotFoundException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
//        SparkConf conf = new SparkConf().setAppName("sql.UserRecordStream").setMaster("local[*]");
//        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));




        // create a mysql database connection
//        String myDriver = "org.gjt.mm.mysql.Driver";
        String myUrl = "jdbc:mysql://localhost/userData";
//        Class.forName(myDriver);


        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("UserRecordStream")
                .getOrCreate();

        Dataset<Row> df = session.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "user")
//                .option("kafka.key.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
//                .option("kafka.value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .load();

        df.createOrReplaceTempView("user_report");


        // id,location,age
        final Dataset<Row>[] res = new Dataset[]{session.sql("select cast(value as string) from user_report")};
//        Object[] arr = new Object[0];

//        // data sink
        StreamingQuery streamingQuery = res[0].writeStream().format("console")
                .outputMode(OutputMode.Append())
//                .foreachBatch(writeToSQL(df))
                .foreachBatch((rowDataset, batchID) -> {
                   if(batchID != null) {

//                       rowDataset.write()
//                               .option("url", "jdbc:mysql://localhost:3306/")
//                               .option("dbtable", "schema.userData")
//                               .option("user", "root")
//                               .option("password", "Hooyo14#")
//                               .saveAsTable("jdbc:mysql://localhost:3306/schema.userData");
                       rowDataset.foreach(row -> {
                           // for each batch we recieved , converted data to json values and further do clean up
                           JsonAST.JValue i = row.jsonValue();
//

//                          // cleaning up data before we insert to sql database.
                           // i dont think this is best practice , better to look for alernative solution
                           // but for now , will do the job.
                           String cleanUpData = i.values().toString().replaceAll("Map", "")
                                   .replaceAll("\\(","")
                                   .replaceAll("value ->","");
                           String[] data = cleanUpData.substring(0,cleanUpData.length() - 1)
                           .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                           String id = data[0];

                           String loc = data[1];

                           String age = data[2];



                           System.out.println("Id: " + id + "\n" + "Loc: " + loc + "\n" + "age: " + age + "\n");
//                           System.out.println("Data is: " + data + "\n");

                           try {
                               Class.forName("com.mysql.cj.jdbc.Driver");
                               Connection conn = DriverManager.getConnection(myUrl, "root", "Hooyo14#");
                               // the mysql insert statement
                               String query = "insert into userReport (userID, location, age)"
                                       + " values (?, ?, ?)";
                               // create the mysql insert preparedstatement
                               PreparedStatement preparedStmt = conn.prepareStatement(query);
                               preparedStmt.setString(1,id);
                               preparedStmt.setString(2,loc);
                               preparedStmt.setString(3,age);
                               // execute the preparedstatement
                               preparedStmt.execute();
                               conn.close();
                           } catch (Exception e) {
                               System.out.println("Error occured connecting to sql " + e + "\n");
                           }

                       });

                   }
                })
                .start();

        streamingQuery.awaitTermination();

//
//        for (Object o : arr) {
//            System.out.println("Arrays object are " + o.toString());
//        }



    }

//    void init(Dataset rowDataset) {
//        rowDataset.foreach(row -> {
//            row.
//            int id = row.fieldIndex(row.getString(1));
//            String loc = row.getString(2);
//            int age = row.fieldIndex(row.getString(3));
//
//            // the mysql insert statement
//            String query = " insert into userReport (userID, location, age)"
//                    + " values (?, ?, ?, ?, ?)";
//
//            // create the mysql insert preparedstatement
//            PreparedStatement preparedStmt = conn.prepareStatement(query);
//            preparedStmt.setString(1,String.valueOf(id));
//            preparedStmt.setString(2,loc);
//            preparedStmt.setString(3,String.valueOf(age));
//
//            // execute the preparedstatement
//            preparedStmt.execute();
//        });
//    }

//    public static VoidFunction2<Dataset<Row>, Long> writeToSQL(Dataset<Row> df) {
//        DataFrameWriter<Row> res =
//
//    }

}

//    public void insertValues(int id,String loc,int a) {
//        try
//        {
//
//
//
//        }
//        catch (Exception e)
//        {
//            System.err.println("Got an exception!");
//            System.err.println(e.getMessage());
//        }
//    }



//
//    // LocationStrategeis.preferConsistent: consumer api that fetches massges into buffers. evenly distributes across available executors
//    Collection<String> topics = Arrays.asList("user");
//    // kafka param https://spark.apache.org/docs/2.1.0/streaming-kafka-0-10-integration.html
////        Map<String,Object> params = new HashMap<>();
//    Map<String, Object> params = new HashMap<>();
//        params.put("bootstrap.servers","localhost:9092"); // list of kafka service connected.
//                params.put("key.deserializer", StringDeserializer.class); // converts java object so kafka deserializer each java object
//        params.put("value.deserializer", StringDeserializer.class); //
//        params.put("group.id","spark-group"); // group more than one consumer. so each events , one group consumes particular message
//        params.put("auto.offset.reset","latest"); // track te latest message consumed. start from beigning or at the latest
//        params.put("enable.auto.commit",false); // automatically commit if it sets = true
//
//        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
//        sc, LocationStrategies.PreferConsistent(),
//        ConsumerStrategies.Subscribe(topics, params)
//        );
//
//        // do work
//        JavaDStream<String> result = stream.map(item -> item.value());
//        result.print();
//
//        sc.start();;
//        sc.awaitTermination();



//
//  if(batchID != null) {
//
//          }