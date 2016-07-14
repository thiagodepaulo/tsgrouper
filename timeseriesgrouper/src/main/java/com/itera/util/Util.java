package com.itera.util;

import scala.Tuple2;

public class Util {
	
	public static int argMin(java.util.Vector<Tuple2<Long, Double>> vec, int maxNN) {
		if (vec.size() < maxNN)
			return vec.size();
		int min = 0;
		for (int i = 1; i < vec.size(); i++)
			if (vec.get(i)._2 < vec.get(min)._2)
				min = i;
		return min;
	}

}
