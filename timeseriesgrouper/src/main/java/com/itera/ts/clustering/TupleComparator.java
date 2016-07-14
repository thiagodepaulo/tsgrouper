package com.itera.ts.clustering;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class TupleComparator implements Comparator<Tuple2<Long, Tuple2<Long, Double>>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<Long, Tuple2<Long, Double>> o1, Tuple2<Long, Tuple2<Long, Double>> o2) {
		if (o1._2._2 > o2._2._2)
			return 1;
		else if (o1._2._2 < o2._2._2)
			return -1;
		else
			return 0;
	}
}
