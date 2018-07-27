package com.sumanth.projects.spark.main;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.sumanth.projects.spark.utils.SparkConnection;

public class MainApp {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = SparkConnection.getSparkConf();
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = jsc.textFile("src/main/resources/dataset2.csv");
		Iterator it = textFile.toLocalIterator();
		Integer wordCount = 0;
		
		//Play around with the RDD
		while (it.hasNext()) {
			String x = (String) it.next();
			wordCount += Arrays.asList(x.split(" ")).size();
		}
		System.out.println("1. Counting the number of words in the text");
		System.out.println(wordCount);
		
		//Map something from the RDD to something else 
		JavaRDD<String> upperCaseTextFile = textFile.map(new Function<String, String>() {
			@Override
			public String call(String v1) throws Exception {
				return v1.toUpperCase();
			}
		});
		List<String> toUpperCasePrintFile = upperCaseTextFile.take(1);
		System.out.println("2. Printing first line uppercase");
		toUpperCasePrintFile.stream().forEach(System.out::print);
		System.out.println();
		
		//Filter something in the RDDs
		JavaRDD<String> filterTextFile = textFile.filter(new Function<String, Boolean> () {

			@Override
			public Boolean call(String v1) throws Exception {
				String[] splitStrings = v1.split(",");
				if (splitStrings[0].equalsIgnoreCase("last_name")) {
					return false;
				} else if (Integer.parseInt(splitStrings[2]) < 200000) {
					return true;
				} else {
					return false;
				}
			}		
		});
		System.out.println("3. Filtering everything out whose salary is > 200k - Printing out count");
		System.out.println(filterTextFile.count());
		
		
		//Reduce the RDDs to find the average of the salaries of the folks
		String sum = textFile.reduce(new Function2<String, String, String>() {

			@Override
			public String call(String v1, String v2) throws Exception {
				if (v1.contains("last_name")||v2.contains("last_name")) {
					return Double.valueOf(0.0).toString();
				}
				else {
					Double sal1 = isNumeric(getSalary(v1))? Double.valueOf(getSalary(v1)):0.0;
					Double sal2 = isNumeric(getSalary(v2))? Double.valueOf(getSalary(v2)):0.0;
					Double sum = sal1 + sal2;
					return sum.toString();
				}
			}
			
			private String getSalary(String in) {
				if (in.contains(",")) {
					String vals[] = in.split(",");
					return vals[2];
				} else {
					return Double.valueOf(0.0).toString();
				}
			}
			
			private boolean isNumeric(String s) {
				return s.matches("[-+]?\\d*\\.?\\d+");
			}
			
		});
		
		Double avgSal = Double.valueOf(sum)/(textFile.count()-1);
		System.out.println("Avg salary is " + avgSal);
	}
}
