package data.cleaning.core.utils;

public class Metrics {
	private int numCorrectRepairs;
	private int numTotalRepairs;
	private int numErrors;

	public int getNumCorrectRepairs() {
		return numCorrectRepairs;
	}

	public void setNumCorrectRepairs(int numCorrectRepairs) {
		this.numCorrectRepairs = numCorrectRepairs;
	}

	public int getNumTotalRepairs() {
		return numTotalRepairs;
	}

	public void setNumTotalRepairs(int numTotalRepairs) {
		this.numTotalRepairs = numTotalRepairs;
	}

	public int getNumErrors() {
		return numErrors;
	}

	public void setNumErrors(int numErrors) {
		this.numErrors = numErrors;
	}

}
