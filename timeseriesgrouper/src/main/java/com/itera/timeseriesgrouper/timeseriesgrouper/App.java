package com.itera.timeseriesgrouper.timeseriesgrouper;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Hello world!
 *
 */
public class App {

	static Metrics metric = new Metrics(Metrics.type.CID);

	public static int argMin(Vector<Tuple2<Long, Double>> vec, int maxNN) {
		if (vec.size() < maxNN)
			return vec.size();
		int min = 0;
		for (int i = 1; i < vec.size(); i++)
			if (vec.get(i)._2 < vec.get(min)._2)
				min = i;
		return min;
	}

	public static final class TupleComparator implements Comparator<Tuple2<Long, Tuple2<Long, Double>>>, Serializable {

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

	public static JavaRDD<Tuple3<Long, Long, Double>> createKnnGraph(JavaPairRDD<Long, Double[]> timeSeries,
			int numNN) {

		Metrics metric = new Metrics();
		// combine all timeseries ids 2 by 2
		JavaPairRDD<Tuple2<Long, Double[]>, Tuple2<Long, Double[]>> pairs = timeSeries.cartesian(timeSeries);
		JavaPairRDD<Long, Tuple2<Long, Double>> edges = pairs.mapToPair(x -> {
			long id1 = x._1._1;
			long id2 = x._2._1;
			Double[] vec1 = x._1._2;
			Double[] vec2 = x._2._2;

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
			Vector<Tuple2<Long, Double>> maxKnn = new Vector<>(numNN);
			int id_min = 0;
			for (Tuple2<Long, Double> tuple : neigList) {
				if (id != tuple._1) {
					id_min = argMin(maxKnn, numNN);
					if (id_min == maxKnn.size())
						maxKnn.add(id_min, tuple);
					else if (tuple._2 > maxKnn.get(id_min)._2) {
						maxKnn.set(id_min, tuple);
					}
				}
			}
			Vector<Tuple3<Long, Long, Double>> edges_list = new Vector<>(numNN);
			for (Tuple2<Long, Double> t : maxKnn) {
				edges_list.addElement(new Tuple3<>(id, t._1, t._2));
			}
			return edges_list;
		});

		return edges3;
	}

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);		
		
		int kGroups = 3;
		int numNN = 5;
		int maxIterations = 40;
		
		SparkConf sparkConf = new SparkConf().setAppName("TS Grouper").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//String arqTS = "/media/thiagodepaulo/Dados/Thiago/ids_synthetic_control.data";
		String arqTS = "/media/thiagodepaulo/Dados/Thiago/tsteste.dat";
		

		JavaPairRDD<Long, Double[]> timeSeries = sc.textFile(arqTS).map(l -> l.split("\\s+")).mapToPair(x -> {
			Double[] ts = new Double[x.length - 1];
			for (int i = 1; i < x.length; i++) {
				ts[i - 1] = Double.parseDouble(x[i]);
			}
			return new Tuple2<>(new Long(x[0]), ts);
		});

		JavaRDD<Tuple3<Long, Long, Double>> edges = createKnnGraph(timeSeries, numNN);
		
		// Cluster the data into two classes using PowerIterationClustering
		PowerIterationClustering pic = new PowerIterationClustering().setK(kGroups).setMaxIterations(maxIterations);
		PowerIterationClusteringModel model = pic.run(edges);

		HashMap<Long, Integer> mapIdClus = new HashMap<>();
		for (PowerIterationClustering.Assignment a : model.assignments().toJavaRDD().collect()) {
			mapIdClus.put(a.id(), a.cluster());			
		}
		System.out.println(mapIdClus);
		System.out.println(edges.collect());
	}
}
