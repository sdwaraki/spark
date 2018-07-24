package com.sumanth.projects.spark.main;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MainApp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word count");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = jsc.textFile("src/main/resources/words.txt");
		Iterator it = textFile.toLocalIterator();
		Integer wordCount = 0;
		while(it.hasNext()) {
			String x = (String) it.next();
			wordCount += Arrays.asList(x.split(" ")).size();
		}
		System.out.println(wordCount);
	}
}
