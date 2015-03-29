package data.cleaning.core.utils.kdtree;


public class NormalizedEuclideanDistance implements DistanceMetric {
	double[] dimToMax;

	public NormalizedEuclideanDistance(double[] dimToMax) {
		this.dimToMax = dimToMax;
	}

	@Override
	public float distance(float[] a, float[] b) {
		if(dimToMax == null || dimToMax.length == 0) {
			return Float.NaN;
		}
		
		double dist = 0d;

		for (int i = 0; i < a.length; i++) {
			double max = dimToMax[i];

			if (max == Double.NaN || max == 0d) {
				dist += Math.pow((a[i] - b[i]), 2) / 0.001d;
			} else {
				dist += Math.pow((a[i] - b[i]) / max, 2);
			}

		}

		return (float)Math.sqrt(dist);
	}

	public float getDistance(float[] a, float[] b) {
		return distance(a, b);
	}
}
