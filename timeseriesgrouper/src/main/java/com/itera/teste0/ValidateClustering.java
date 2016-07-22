package com.itera.teste0;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.itera.ts.clustering.Loader;
import com.itera.ts.clustering.TSClustering;
import com.itera.ts.clustering.TSClustering.algType;
import com.itera.util.Metrics;
import com.itera.util.Metrics.type;

public class ValidateClustering {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		int kGroups = 6;
		int numNN = 50;
		int maxIterations = 10;

		JavaPairRDD<Long, double[]> timeSeries = Loader.loadFromFile();
		// JavaPairRDD<Long, double[]> timeSeries = Loader.loadFromCassandra();

		// clustering
		TSClustering tsclus = new TSClustering(kGroups, algType.GRAPH_CLUSTERING, new Metrics(type.DTW));
		// TSClustering tsclus = new TSClustering(kGroups, algType.KMEANS);
		tsclus.doClustering(timeSeries, numNN, maxIterations);

		// clustering results. assignment <TS_ID, CLUSTER_ID>
		JavaPairRDD<Long, Integer> assig = tsclus.getClusterAssignment();

		Map<Integer, Iterable<Long>> map = tsclus.getClustersMembers();

		Map<Integer, Iterable<Long>> mapReal = new HashMap<>();
		for (int i = 0; i < 6; i++) {
			mapReal.put(i, LongStream.rangeClosed((i * 100) + 1, (i + 1) * 100)::iterator);
		}
		
		double r = randIndex(map, mapReal);
		System.out.println(r);
	}

	public static double randIndex(Map<Integer, Iterable<Long>> mapPred, Map<Integer, Iterable<Long>> mapReal) {
		HashMap<Long, Integer> invPred = new HashMap<>();
		mapPred.forEach((x, y) -> {
			for (Long f : y)
				invPred.put(f, x);
		});
		HashMap<Long, Integer> invReal = new HashMap<>();
		mapReal.forEach((x, y) -> {
			for (Long f : y)
				invReal.put(f, x);
		});
		int a, b, c, d;
		a = b = c = d = 0;
		System.out.println(invPred.size());
		System.out.println(invReal.size());

		for (Long k1 : invPred.keySet()) {
			for (Long k2 : invPred.keySet()) {
				if (k1 != k2) {
					int k1Pred = invPred.get(k1);
					int k2Pred = invPred.get(k2);
					int k1Real = invReal.get(k1);
					int k2Real = invReal.get(k2);
					if (k1Pred == k2Pred && k1Real == k2Real)
						a++;
					if (k1Pred != k2Pred && k1Real != k2Real)
						b++;
					if (k1Pred == k2Pred && k1Real != k2Real)
						c++;
					if (k1Pred != k2Pred && k1Real == k2Real)
						d++;
				}
			}
		}
		a /= 2.;
		b /= 2.;
		c /= 2.;
		d /= 2.;
		System.out.println("a=" + a + " b=" + b + " c=" + c + " d=" + d);
		return ((double)a + b) / (a + b + c + d);
	}

}
