package com.sumanth.projects.spark.ml;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sumanth.projects.spark.utils.SparkConnection;

public class LinearRegressionTrial {

	public static void main(String args[]) {
		// ------------------initz-----
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = SparkConnection.getSparkConf();
		SparkSession session = SparkConnection.getSparkSession();
		JavaSparkContext context = SparkConnection.getJavaSparkContext();

		Dataset<Row> autoDf = session.read().option("header", true)
				.csv("/Users/sumanth/Documents/workspace/autodataset.csv");

		Dataset<Row> autoDfCleansed = autoDf.select(col("*")).filter(col("price").notEqual("?"))
				.filter(col("num-of-doors").notEqual("?")).filter(col("bore").notEqual("?"))
				.filter(col("stroke").notEqual("?")).filter(col("horsepower").notEqual("?"))
				.filter(col("peak-rpm").notEqual("?"));


		JavaRDD<Row> autoDfWithDataTypes = autoDfCleansed.toJavaRDD().map(new Function<Row, Row>() {

			@Override
			public Row call(Row v1) throws Exception {
				return RowFactory.create(v1.getString(1), Double.valueOf(v1.getString(8)),
						Double.parseDouble(v1.getString(9)), Double.parseDouble(v1.getString(10)), Double.parseDouble(v1.getString(11)),
						Double.parseDouble(v1.getString(12)), Double.parseDouble(v1.getString(15)), Double.parseDouble(v1.getString(17)),
						Double.parseDouble(v1.getString(18)), Double.parseDouble(v1.getString(19)), Double.parseDouble(v1.getString(20)),
						Double.parseDouble(v1.getString(21)), Double.parseDouble(v1.getString(22)), Double.parseDouble(v1.getString(23)),
						Double.parseDouble(v1.getString(24))
				);
			}

		});


		StructType autoSchema = DataTypes.createStructType(
				new StructField[] {
						DataTypes.createStructField("make", DataTypes.StringType, false),
						DataTypes.createStructField("wheel-base", DataTypes.DoubleType, false),
						DataTypes.createStructField("length", DataTypes.DoubleType, false),
						DataTypes.createStructField("width", DataTypes.DoubleType, false),
						DataTypes.createStructField("height", DataTypes.DoubleType, false),
						DataTypes.createStructField("curb-weight", DataTypes.DoubleType, false),
						DataTypes.createStructField("engine-size", DataTypes.DoubleType, false),
						DataTypes.createStructField("bore", DataTypes.DoubleType, false),
						DataTypes.createStructField("stroke", DataTypes.DoubleType, false),
						DataTypes.createStructField("compression-ratio", DataTypes.DoubleType, false),
						DataTypes.createStructField("horsepower", DataTypes.DoubleType, false),
						DataTypes.createStructField("peak-rpm", DataTypes.DoubleType, false),
						DataTypes.createStructField("city-mpg", DataTypes.DoubleType, false),
						DataTypes.createStructField("highway-mpg", DataTypes.DoubleType, false),
						DataTypes.createStructField("price", DataTypes.DoubleType, false)
				});
		//Repartition the stuff
		JavaRDD<Row> autoDfRepartition = autoDfCleansed.toJavaRDD().repartition(2);

		//Create DataFrame back with the new schema
		Dataset<Row> autoDfWithSchema = session.createDataFrame(autoDfWithDataTypes, autoSchema);


		//-----------Analyze the data -------
		for (StructField field : autoSchema.fields()) {
			if(!field.dataType().equals(DataTypes.StringType)) {
				System.out.println("Correlation between price and " + field.name() + " = "+autoDfWithSchema.stat().corr("price", field.name()));
			}
		}







	}
}
