package com.sumanth.projects.spark.utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

import scala.Tuple2;

public class MinTupleFinder implements Comparator<Tuple2<String,BigInteger>>,Serializable {

	@Override
	public int compare(Tuple2<String, BigInteger> o1, Tuple2<String, BigInteger> o2) {
		return o1._2.compareTo(o2._2);
	}


}
