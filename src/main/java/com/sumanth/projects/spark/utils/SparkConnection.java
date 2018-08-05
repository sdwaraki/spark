package com.sumanth.projects.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConnection {

	private static JavaSparkContext javaSparkContext = null;
	private static SparkSession sparkSession = null;
	private static SparkConf conf = null;

	public static void getConnection() {
		if (javaSparkContext == null) {
			conf = new SparkConf().setMaster("local").setAppName("local");
			javaSparkContext = new JavaSparkContext(getSparkConf());
			sparkSession = SparkSession.builder().appName("local").master("local").config("spark.sql.warehouse.dir",
					"file:/Users/sumanth/Documents/workspace/spark/src/main/resources/temp").getOrCreate();
		}
	}

	public static SparkConf getSparkConf() {
		if (conf == null) {
			getConnection();
		}
		return conf;
	}

	public static JavaSparkContext getJavaSparkContext() {
		if (javaSparkContext == null) {
			getConnection();
		}
		return javaSparkContext;
	}

	// spark ware house directory is used for spark to create temp files while doing
	// some table ops - Need this value
	// to be set to something on the file system.
	public static SparkSession getSparkSession() {
		if (sparkSession == null) {
			getConnection();
		}
		return sparkSession;
	}
}
