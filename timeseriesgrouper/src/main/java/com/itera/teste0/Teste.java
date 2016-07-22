package com.itera.teste0;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.year;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import scala.Tuple2;
import scala.Tuple3;

public class Teste {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String cassandraHost = "127.0.0.1";
		String cassandraUsername = "itera";
		String cassandraPassword = "*lab.gaia";
		String sparkHost = "local";
		String appName = "teste";
		String keyspace = "itera_miner";
		String query = "select * from event_cc";

		SparkConf conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
				.set("spark.cassandra.auth.username", cassandraUsername)
				.set("spark.cassandra.auth.password", cassandraPassword);

		JavaSparkContext sc = new JavaSparkContext(sparkHost, appName, conf);

		CassandraSQLContext sqlContext = new CassandraSQLContext(sc.sc());
		sqlContext.setKeyspace(keyspace);
		DataFrame df = sqlContext.cassandraSql(query);

		df = df.select(col("id"), col("event_start_dt"), col("event_amt"));

		DataFrame df_grouped = df.groupBy(col("id"), month(col("event_start_dt")), year(col("event_start_dt")))
				.sum("event_amt");
		for (String s : df_grouped.columns())
			System.out.println(s);
		// df_grouped.orderBy(year(col("event_start_dt")),
		// month(col("event_start_dt")), col("id")).show(20);
		// df_grouped.orderBy(col("month")).show(20);
		// df_grouped.selectExpr("id=8742831").show(10);

		JavaPairRDD<Long, Iterable<Tuple3<Integer, Integer, Double>>> clients_RDD = df_grouped.javaRDD().mapToPair(x -> {
			return new Tuple2<Long, Tuple3<Integer, Integer, Double>>((Long) x.get(0),
					new Tuple3<>((Integer) x.get(1), (Integer) x.get(2), (Double) x.get(3)));
		}).groupByKey();
		
		
		clients_RDD.foreach(x -> {
			System.out.println(x._1);
			System.out.println(x._2);
		});
		
		// df_grouped.javaRDD().foreach(x -> {System.out.println(x);});
		
	}

}
