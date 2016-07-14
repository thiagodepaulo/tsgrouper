package com.itera.util;

public class Metrics implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public enum type {
		EUCLIDEAN_DIST, DTW, CID
	}

	public type metricType;

	public Metrics() {
		this(type.EUCLIDEAN_DIST);
	}

	public Metrics(type metricType) {
		this.metricType = metricType;
	}

	public double dist(double[] a1, double[] a2, Object... params) {
		switch (this.metricType) {
		case EUCLIDEAN_DIST:
			return this.euclidean(a1, a2);
		case DTW:
			return this.dtw(a1, a2);
		case CID:
			return this.cid(a1, a2);
		default:
			return 0;
		}
	}
	
	public double euclidean(double[] a1, double[] a2) {
		if (a1.length != a2.length)
			throw new RuntimeException("Time series of different size.");
		double sum = 0;
		for (int i = 0; i < a1.length; i++) {
			sum += Math.pow(a1[i] - a2[i], 2);
		}
		return Math.sqrt(sum);
	}

	private double ce(double[] a) {
		double v = 0;
		for (int i = 0; i < a.length - 1; i++) {
			v += Math.pow(a[i] - a[i + 1], 2);
		}
		return Math.sqrt(v);
	}

	public double cid(double[] a1, double[] a2) {
		double ce1 = ce(a1);
		double ce2 = ce(a2);
		double cf = Math.max(ce1, ce2) / Math.min(ce1, ce2);
		return this.euclidean(a1, a2) * cf;
	}

	public double dtw(double[] a1, double[] a2) {
		int n = a1.length;
		int m = a2.length;
		double[][] D = new double[n][m];

		for (int i = 1; i < n; i++)
			D[i][0] = Double.POSITIVE_INFINITY;
		for (int i = 1; i < m; i++)
			D[0][i] = Double.POSITIVE_INFINITY;
		double cost;
		for (int i = 1; i < n; i++) {
			for (int j = 1; j < n; j++) {
				cost = Math.abs(a1[i] - a2[j]);
				D[i][j] = cost + Math.min(Math.min(D[i - 1][j], D[i][j - 1]), D[i - 1][j - 1]);
			}
		}
		return D[n - 1][m - 1];
	}
}
