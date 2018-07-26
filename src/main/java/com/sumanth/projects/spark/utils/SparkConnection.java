package com.sumanth.projects.spark.utils;

import org.apache.spark.SparkConf;

public class SparkConnection {
	public static SparkConf getSparkConf() {
		return new SparkConf().setMaster("local").setAppName("local");
	}
	
}
