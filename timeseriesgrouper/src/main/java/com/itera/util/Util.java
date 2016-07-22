package com.itera.util;

import java.util.HashMap;

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

	/**
	 * Map each time to integer: Ex.: Suppose an initial time ti and an ending
	 * time te. The time ti is defined by year*100+month. ti = 201602
	 * (correspond to year 2016 and month 02) te = 201710 (corresponde to year
	 * 2017 and month 10). The mapping is created as 201602=0, 201603=1,
	 * 201604=2, ..., 201709=19, 201710=20
	 * 
	 * @param init
	 *            initial time
	 * @param end
	 *            ending time
	 * @return return a mapping to time to each integer.
	 */
	public static HashMap<Integer, Integer> mapTimeToInt(int init, int end) {
		HashMap<Integer, Integer> map = new HashMap<>();
		int t = init;
		int i = 0;
		do {
			map.put(t, i++);
			t = (t % 100) % 12 == 0 ? ((t / 100) + 1) * 100 + 1 : t + 1;
		} while (t <= end);
		return map;
	}

}
