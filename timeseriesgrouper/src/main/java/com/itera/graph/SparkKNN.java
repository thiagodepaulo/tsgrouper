package com.itera.graph;

import java.nio.file.Files;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.itera.ts.clustering.TupleComparator;
import com.itera.util.Metrics;
import com.itera.util.Util;

import scala.Tuple2;
import scala.Tuple3;

public class SparkKNN {
	
	public static JavaRDD<Tuple3<Long, Long, Double>> createKnnGraph(JavaPairRDD<Long, double[]> timeSeries, int numNN,
			Metrics metric) {

		// combine all timeseries ids 2 by 2
		JavaPairRDD<Tuple2<Long, double[]>, Tuple2<Long, double[]>> pairs = timeSeries.cartesian(timeSeries);
		JavaPairRDD<Long, Tuple2<Long, Double>> edges = pairs.mapToPair(x -> {
			long id1 = x._1._1;
			long id2 = x._2._1;
			double[] vec1 = x._1._2;
			double[] vec2 = x._2._2;

			return new Tuple2<Long, Tuple2<Long, Double>>(id1, new Tuple2<Long, Double>(id2, metric.dist(vec1, vec2)));
		});
		// get the max distance betweeen nodes
		Tuple2<Long, Tuple2<Long, Double>> maxT = edges.max(new TupleComparator());
		Double maxD = maxT._2._2;
		edges = edges.mapToPair(x -> {
			return new Tuple2<>(x._1, new Tuple2<>(x._2._1, maxD - x._2._2));
		});

		// create a neighbours list for ead ts_id
		JavaPairRDD<Long, Iterable<Tuple2<Long, Double>>> neigbours = edges.groupByKey();
		// calc the similarity between ts_id's and create a typle (ts_id1,
		// ts_id1, similarity)
		JavaRDD<Tuple3<Long, Long, Double>> edges3 = neigbours.flatMap(x -> {
			Long id = x._1;
			Iterable<Tuple2<Long, Double>> neigList = x._2;
			java.util.Vector<Tuple2<Long, Double>> maxKnn = new java.util.Vector<>(numNN);
			int id_min = 0; 
			for (Tuple2<Long, Double> tuple : neigList) {
				if (id != tuple._1) {
					id_min = Util.argMin(maxKnn, numNN);
					if (id_min == maxKnn.size())
						maxKnn.add(id_min, tuple);
					else if (tuple._2 > maxKnn.get(id_min)._2) {
						maxKnn.set(id_min, tuple);
					}
				}
			}
			java.util.Vector<Tuple3<Long, Long, Double>> edges_list = new java.util.Vector<>(numNN);
			for (Tuple2<Long, Double> t : maxKnn) {
				edges_list.addElement(new Tuple3<>(id, t._1, t._2));
			}
			return edges_list;
		});
		return edges3;
	}

}
