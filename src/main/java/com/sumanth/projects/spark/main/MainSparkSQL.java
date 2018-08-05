package com.sumanth.projects.spark.main;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.sumanth.projects.spark.utils.SparkConnection;

public class MainSparkSQL {
	public static void main(String args[]) {
		// ------------------initz-----
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = SparkConnection.getSparkConf();
		SparkSession session = SparkConnection.getSparkSession();
		JavaSparkContext context = SparkConnection.getJavaSparkContext();

		// Read Json - Source http://jmcauley.ucsd.edu/data/amazon/
		Dataset<Row> reviewsDF = session.read().json("/Users/sumanth/Documents/workspace/reviews_Digital_Music_5.json");

		// Show all but limit to 10
		reviewsDF.limit(10).show();

		// select asin,overall,reviewerId from dataframe where overall<4 limit 20
		reviewsDF.select(col("asin"), col("overall"), col("reviewerID"))
				.filter(col("overall").$less(4)).limit(20).show();

		// select count(*) from dataframe where asin='B0000000ZW'
		Long something = reviewsDF.select().filter(col("asin").equalTo("B0000000ZW")).count();
		System.out.println("The count is  " + something);
		
//		//Update the column data
//		Dataset<Row> reviewTimeDF = reviewsDF.select(col("reviewTime"));
//		JavaRDD<Row> reviewTimeRDD = reviewTimeDF.toJavaRDD();
//		reviewTimeRDD.map(new Function<Row, Row>() {
//
//			@Override
//			public Row call(Row v1) throws Exception {
//				String curDate = (String) v1.get(0);
//				String parts[] = curDate.split(",");
//				String moreParts[] = parts[0].split(" ");
//				String finalDate = moreParts[0]+"/"+moreParts[1]+"/"+parts[1];				
//				v1.
//			}
//			
//		});
	}
}
