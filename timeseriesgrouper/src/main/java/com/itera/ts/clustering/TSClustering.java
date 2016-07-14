package com.itera.ts.clustering;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.itera.graph.SparkKNN;
import com.itera.util.Metrics;

import scala.Tuple2;
import scala.Tuple3;

public class TSClustering {

	public enum algType {
		GRAPH_CLUSTERING, KMEANS
	}

	private int K;
	private algType alg;
	private JavaPairRDD<Long, Integer> assignment;
	public Metrics metric;

	public TSClustering(int K, algType alg, Metrics metric) {
		this.K = K;
		this.alg = alg;
		this.metric = metric;
	}
 
	public TSClustering(int K) {
		this(K, algType.GRAPH_CLUSTERING, new Metrics(Metrics.type.EUCLIDEAN_DIST));
	}
	
	public TSClustering(int K, algType alg) {
		this(K, alg, new Metrics(Metrics.type.EUCLIDEAN_DIST));
	}

	public void doClustering(Object... args) {
		JavaPairRDD<Long, double[]> timeSeries = (JavaPairRDD<Long, double[]>) args[0];
		if (this.alg == algType.GRAPH_CLUSTERING) {			
			int numNN = (int) args[1]; // number of nearest neighbours
			int maxIterations = 40;
			if (args.length > 2) {
				maxIterations = (int) args[2];
			}
			graphClustering(timeSeries, numNN, maxIterations);
		} else if (this.alg == algType.KMEANS) {			
			int maxIterations = 50;
			if (args.length > 1) {
				maxIterations = (int) args[1];
			}
			kMeans(timeSeries, maxIterations);
		}
	}

	public void graphClustering(JavaPairRDD<Long, double[]> timeSeries, int numNN, int maxIterations) {
		// create KNN graph with numNN
		JavaRDD<Tuple3<Long, Long, Double>> knn = SparkKNN.createKnnGraph(timeSeries, numNN, metric);
		// Cluster the data into two classes using PowerIterationClustering
		PowerIterationClustering pic = new PowerIterationClustering().setK(K).setMaxIterations(maxIterations);
		PowerIterationClusteringModel model = pic.run(knn);

		assignment = model.assignments().toJavaRDD().mapToPair(x -> {
			return new Tuple2<Long, Integer>(x.id(), x.cluster());
		});
	}

	public void kMeans(JavaPairRDD<Long, double[]> timeSeries, int maxIterations) {
		JavaRDD<Vector> vecTS = timeSeries.map(x -> {
			return Vectors.dense(x._2);
		});
		KMeansModel model = KMeans.train(vecTS.rdd(), this.K, maxIterations);
		assignment = timeSeries.mapToPair(x -> {
			Vector point = Vectors.dense(x._2);
			int clusId = model.predict(point);
			return new Tuple2<Long, Integer>(x._1, clusId);
		}); 
	}		

	public JavaPairRDD<Long, Integer> getClusterAssignment() {
		return this.assignment;
	}
}
