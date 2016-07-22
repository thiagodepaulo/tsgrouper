package com.itera.ts.clustering;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.year;

import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.DataFrame;

import com.itera.ts.clustering.IteraSparkConf.Source;
import com.itera.util.Util;

import scala.Tuple2;
import scala.Tuple3;

public class Loader {

	public static JavaPairRDD<Long, double[]> loadFromFile() {
		IteraSparkConf iteraConf = new IteraSparkConf(Source.FILE, "TS Grouper", "local");
		String arqTS = "/media/thiagodepaulo/Dados/Thiago/ids_synthetic_control.data";
		// String arqTS = "/media/thiagodepaulo/Dados/Thiago/tsteste.dat";
		return iteraConf.loadFile(arqTS).map(l -> l.split("\\s+")).mapToPair(x -> {
			double[] ts = new double[x.length - 1];
			for (int i = 1; i < x.length; i++) {
				ts[i - 1] = Double.parseDouble(x[i]);
			}
			return new Tuple2<>(new Long(x[0]), ts);
		});
	}

	public static JavaPairRDD<Long, double[]> loadFromCassandra() {
		IteraSparkConf iteraConf = new IteraSparkConf(Source.CASSANDRA, "TS Grouper", "local");
		DataFrame df = iteraConf.loadCassandra();
		// selec columns "id", "event_start_dt", "event_amt"
		// and group by "id", months and years
		df = df.select(col("id"), col("event_start_dt"), col("event_amt"))
				.groupBy(col("id"), month(col("event_start_dt")), year(col("event_start_dt"))).sum("event_amt");

		// Map each row ("id", month, year, "event_amt") to a tuple ("id",
		// (month, year, "event_amt"))
		// and group by "id". Its creates a list for each client "id"
		JavaPairRDD<Long, Iterable<Tuple3<Integer, Integer, Double>>> clients_RDD = df.javaRDD().mapToPair(x -> {
			return new Tuple2<Long, Tuple3<Integer, Integer, Double>>((Long) x.get(0),
					new Tuple3<>((Integer) x.get(1), (Integer) x.get(2), (Double) x.get(3)));
		}).groupByKey();

		// time interval 201401 (01/2014) to 201512 (12/2015)
		int init = 201401;
		int end = 201512;
		HashMap<Integer, Integer> mapTimeToInt = Util.mapTimeToInt(init, end);
		int nInterval = mapTimeToInt.size();

		JavaPairRDD<Long, double[]> ts = clients_RDD.mapToPair(x -> {
			double[] serie = new double[nInterval];
			Long id = x._1;
			for (Tuple3<Integer, Integer, Double> t : x._2) {
				int month = t._1();
				int year = t._2();
				double value = t._3();
				int tkey = year * 100 + month;
				if (mapTimeToInt.containsKey(tkey))
					serie[mapTimeToInt.get(tkey)] = value; 
			}
			return new Tuple2<Long, double[]>(id, serie);
		});
		return ts;
	}



}
