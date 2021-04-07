import au.com.bytecode.opencsv.CSVWriter;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class BooksRecommendation {
    public static void main(String[] args) throws IOException {
        List<String[]> list = new ArrayList<>();
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("RatingReportStream")
                .config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/rating.ratingData")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/rating.ratingData")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // read data from mongodb
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        dataPrep(rdd, list, false);

        Dataset<Row> csvData = spark
                .read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("src/main/resources/replicaDataset.csv");
//        csvData.printSchema();


        // get rid of all data with null values
        for(String cols : csvData.columns()) {
            csvData = csvData.withColumn(cols,when(col(cols).isNull(), 0).otherwise(col(cols)));
        }

        // get rid of all rating equal to 0
        csvData = csvData.filter("rating != 0 ");


        csvData = new StringIndexer()
                .setInputCol("ISBN")
                .setOutputCol("bookIdIndexer")
                .fit(csvData)
                .transform(csvData);

        // data training
        Dataset<Row>[] dataSplit = csvData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainedData = dataSplit[0]; // 80 % trained
        Dataset<Row> testData = dataSplit[1]; // 20 % test


        ALS als = new ALS();

        // Build the recommendation model using ALS on the csv data
        als.setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("bookIdIndexer")
                .setRatingCol("rating");

        // recommendation model without training data
        ALSModel model1 = als.fit(csvData);

        // recommendation model with training data
        ALSModel model2 = als.fit(trainedData);

//        csvData.show();

        // test data
        Dataset<Row> prediction = model2.transform(testData);

//        prediction.show();
        // drop prediction not needed. as it shows meaningless value
        prediction = prediction.drop("prediction");
//
//        // prediction model for test data
        ALSModel predictionModel = als.fit(prediction);


        // drop any data that we are not enable to show recommended data
        model2.setColdStartStrategy("drop");
        model1.setColdStartStrategy("drop");
//
//        // Generate top 5 books recommendations for each user
        Dataset<Row> userRecs = model1.recommendForAllUsers(5);
        List<Row> userRecsList = userRecs.takeAsList(5);

        System.out.println("Model 1: Recommendation model without training data \n");
        System.out.println("****************************************************\n");
        System.out.println();

        for(Row row : userRecsList) {
            int userID = row.getAs(0);
            String recs = row.getAs(1).toString();
            System.out.println("User " + userID + " we might want to recommend " + recs);
            System.out.println("This user has already read following books:");
            csvData.filter("userId = " + userID).show();
        }

        Dataset<Row> userModelRecs = predictionModel.recommendForAllUsers(5);
        List<Row> modelRecsList = userModelRecs.takeAsList(5);

        System.out.println("Model 2: Recommendation model with trained data \n");
        System.out.println("****************************************************\n");
        System.out.println();

        for(Row row : modelRecsList) {
            int userID = row.getAs(0);
            String recs = row.getAs(1).toString();
            System.out.println("User " + userID + " we might want to recommend " + recs);
            System.out.println("This user has already read following books:");
            csvData.filter("userId = " + userID).show();
       }

        System.out.println("Model 1: Evaluating recommendation model without training data \n");

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("bookIdIndexer");

        double r2 = evaluator.evaluate(csvData);

        System.out.println("root mean square error = " + r2 + "\n");
        System.out.println("****************************************************");
        System.out.println();


        System.out.println("Model 2: Evaluating recommendation model with training data \n");

        RegressionEvaluator evaluator2 = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("bookIdIndexer");

        double r2_2 = evaluator2.evaluate(prediction);

        System.out.println("root mean square error = " + r2_2 + "\n");
        System.out.println("****************************************************");

        jsc.close();

    }

    private static void dataPrep(JavaMongoRDD<Document> rdd,List<String[]> csvData, boolean isRead) throws IOException {

        if(isRead) {
            String[] header = {"userId","ISBN","rating"};
            csvData.add(header);
            rdd.foreach(document -> {
                for (Object e : document.values()) {
                    if(!e.toString().contains("603")) {

                        String[] data = e.toString().split(",");

                        String userId = data[0];

                        String ISBN = data[1].replaceAll("[a-zA-Z]*","0");

                        String rating = data[2];

                        csvData.add(new String[] {userId,ISBN,rating});

                        insertCSVData(csvData);

//                        System.out.println("userId: " + userId + "\n" + "ISBN: " + ISBN + "\n" + "rating: " + rating + "\n");
                    }

                }
            });
        }

    }

    private static void insertCSVData(List<String[]> csvData) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/replicaDataset.csv"))) {
            writer.writeAll(csvData);
        }
    }
}






