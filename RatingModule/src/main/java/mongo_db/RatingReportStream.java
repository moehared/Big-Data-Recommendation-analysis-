package mongo_db;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.bson.Document;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RatingReportStream implements Serializable {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //myCollection
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("RatingReportStream")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/rating.ratingData")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/rating.ratingData")
                .getOrCreate();
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        String mongoDbFormat = "com.mongodb.kafka.connect.source.json.formatter.DefaultJson";

        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "rating")
                .load();
//        df.selectExpr("CAST(value as string) as rating_data");
//


//        Map<String, String> writeOverrides = new HashMap<String, String>();
//        writeOverrides.put("collection", "spark");
//        writeOverrides.put("writeConcern.w", "majority");
//        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);


//        df.writeStream().format(mongoDbFormat).mode(SaveMode.Append).save();
        df.createOrReplaceTempView("rating_table");
        Dataset<Row> res = spark.sql("select cast(value as string) from rating_table");
//        res.show();
//        df.select(col("value").cast(DataTypes.StringType)).as("rating_data");



        StreamingQuery outQuery = res.writeStream()
                .format("console")
                .option(mongoDbFormat,"mongodb://127.0.0.1/rating.ratingData")
                .outputMode(OutputMode.Append())
                .foreachBatch((rowDataset, batchID) -> {
                    if(batchID != null) {
                        MongoSpark.save(rowDataset);
                    }
                })
                .start();

//        MongoSpark.save((JavaRDD<Document>) outQuery);

        outQuery.awaitTermination();

//        jsc.close();


    }

}
