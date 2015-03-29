package data.cleaning.core.utils.search;

public class RandomWalkStats {
	private double min;
	private double max;
	private double mean;

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getMean() {
		return mean;
	}

	public void setMean(double mean) {
		this.mean = mean;
	}

	@Override
	public String toString() {
		return "RandomWalkStats [min=" + min + ", max=" + max + ", mean="
				+ mean + "]";
	}
}
