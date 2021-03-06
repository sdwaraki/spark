package com.sumanth.projects.spark.main;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.sumanth.projects.spark.utils.MinTupleFinder;
import com.sumanth.projects.spark.utils.SparkConnection;

import scala.Tuple2;

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
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

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

			private static final long serialVersionUID = 1L;

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
				if (v1.contains("last_name") || v2.contains("last_name")) {
					return "0";
				} else {
					BigInteger sal1 = BigInteger.valueOf(getSalary(v1));
					BigInteger sal2 = BigInteger.valueOf(getSalary(v2));
					BigInteger sum = sal1.add(sal2);
					return sum.toString();
				}
			}

			private Long getSalary(String in) {
				if (in.contains(",")) {
					String vals[] = in.split(",");
					return Long.valueOf(vals[2]);
				} else {
					return Long.valueOf(in);
				}
			}

		});
		
		System.out.println("Total Salary: " + Long.valueOf(sum));
		System.out.println("Total Employees: " + textFile.count());
		Double avgSal = Double.valueOf(sum) / (textFile.count() - 1);
		System.out.println("Avg salary is " + avgSal);
		
		
		//Trying something with FlatMap - Flatmap takes an RDD of a certain type and spits out one or more things that can be used as a element of a collection. 
		List<String> flatMapResult = textFile.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				String[] splitStrings = t.split(",");
				return Arrays.asList(splitStrings).iterator();
			}
			
		}).collect();
		
		System.out.println("Total number of words in the file is " + flatMapResult.size());
		
		//Using the Map Functions to emit tuples and reduce by key operation
		JavaPairRDD<String, BigInteger> presidentSalary = textFile.filter(new Function<String, Boolean> () {

			@Override
			public Boolean call(String v1) throws Exception {
				String[] splitStrings = v1.split(",");
				if (splitStrings[4].contains("President")) {
					return true;
				}
				return false;
			}
			
		}).mapToPair(new PairFunction<String, String, BigInteger>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, BigInteger> call(String t) throws Exception {
				String[] splitStrings = t.split(",");
				return new Tuple2<String, BigInteger>(splitStrings[4], BigInteger.valueOf(Long.parseLong(splitStrings[2])));
			}
		});
		
		JavaPairRDD<String,BigInteger> totalPrezSalary = presidentSalary.reduceByKey(new Function2<BigInteger, BigInteger, BigInteger>() {

			@Override
			public BigInteger call(BigInteger v1, BigInteger v2) throws Exception {
				return v1.add(v2);
			}
			
		});
		
		List<Tuple2<String,BigInteger>>something = totalPrezSalary.take(40);
		something.forEach(s -> System.out.println(s._1+" --- "+s._2));
		
	    Tuple2<String, BigInteger> min =  totalPrezSalary.min(new MinTupleFinder());
	    
	    
	    System.out.println(min._1+ "...."+min._2);
	}
}
