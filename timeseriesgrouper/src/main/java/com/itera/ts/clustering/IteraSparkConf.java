package com.itera.ts.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

public class IteraSparkConf extends SparkConf {

	public enum Source {
		CASSANDRA, FILE;
	}

	public Source source;
	public JavaSparkContext sc;

	public IteraSparkConf(Source source, String appName, String master) {
		super(true);
		this.source = source;
		if (this.source == Source.CASSANDRA) {
			confCassandra(appName, master);
		} else if (this.source == Source.FILE) {
			confFile(appName, master);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T load(Object... args) {
		if (this.source == Source.CASSANDRA) {
			return (T) loadCassandra(args);
		} else if (this.source == Source.FILE) {
			return (T) loadFile(args);
		}
		return null;
	}

	private void confFile(String name, String master) {
		this.setAppName(name).setMaster(master);
	}

	private void confCassandra(String appName, String master) {
		String cassandraHost = "127.0.0.1";
		String cassandraUsername = "itera";
		String cassandraPassword = "*lab.gaia";

		this.set("spark.cassandra.connection.host", cassandraHost)
				.set("spark.cassandra.auth.username", cassandraUsername)
				.set("spark.cassandra.auth.password", cassandraPassword).setMaster(master).setAppName(appName);
	}

	public DataFrame loadCassandra(Object... args) {
		String sparkHost = "local";
		String appName = "teste";
		String keyspace = "itera_miner";
		String query = "select * from event_cc";

		this.sc = new JavaSparkContext(sparkHost, appName, this);
		CassandraSQLContext sqlContext = new CassandraSQLContext(sc.sc());
		sqlContext.setKeyspace(keyspace);
		DataFrame df = sqlContext.cassandraSql(query);
		return df;
	}

	public JavaRDD<String> loadFile(Object... args) {
		this.sc = new JavaSparkContext(this);
		String arqName = (String)args[0];
		return this.sc.textFile(arqName);
	}

}
