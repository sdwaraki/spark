package com.sumanth.projects.spark.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import com.sumanth.projects.spark.utils.SparkConnection;

public class MainSparkSQL {
	public static void main(String args[]) throws InterruptedException {
		// ------------------initz-----
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = SparkConnection.getSparkConf();
		SparkSession session = SparkConnection.getSparkSession();
		JavaSparkContext context = SparkConnection.getJavaSparkContext();
		
		

		// Read Json - Source http://jmcauley.ucsd.edu/data/amazon/
		Dataset<Row> reviewsDF = session.read().json("/Users/sumanth/Documents/workspace/reviews_Digital_Music_5.json");
		reviewsDF.printSchema();
	
		// Show all but limit to 10
		reviewsDF.limit(10).show();

		// select asin,overall,reviewerId from dataframe where overall<4 limit 20
		reviewsDF.select(col("asin"), col("overall"), col("reviewerID"))
				.filter(col("overall").$less(4)).limit(20).show();

		 //select count(*) from dataframe where asin='B0000000ZW'
		Long something = reviewsDF.select().filter(col("asin").equalTo("B0000000ZW")).count();
		System.out.println("The count is  " + something);

	}
	
}
