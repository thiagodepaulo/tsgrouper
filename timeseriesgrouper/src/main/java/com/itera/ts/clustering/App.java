package com.itera.ts.clustering;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.itera.ts.clustering.TSClustering.algType;
import com.itera.util.Metrics;
import com.itera.util.Metrics.type;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App {


	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		int kGroups = 100;
		int numNN = 5;		
		// int maxIterations = 40;

		// JavaPairRDD<Long, double[]> timeSeries = Loader.loadFromFile();
		JavaPairRDD<Long, double[]> timeSeries = Loader.loadFromCassandra();

		// clustering
		TSClustering tsclus = new TSClustering(kGroups, algType.GRAPH_CLUSTERING, new Metrics(type.DTW));
		//TSClustering tsclus = new TSClustering(kGroups, algType.KMEANS);
		tsclus.doClustering(timeSeries, numNN);

		// clustering results. assignment <TS_ID, CLUSTER_ID>
		JavaPairRDD<Long, Integer> assig = tsclus.getClusterAssignment();

		Map<Integer, Iterable<Long>> map = assig.mapToPair(x -> {
			return new Tuple2<Integer, Long>(x._2, x._1);
		}).groupByKey().collectAsMap();
		Path path = Paths.get("out");
		try (BufferedWriter writer = Files.newBufferedWriter(path)) {
			for (int i = 0; i < kGroups; i++) {
				for (long id: map.get(i))
					writer.write(id+" ");
				writer.write("\n");
			}		    
		}		
	}
}
