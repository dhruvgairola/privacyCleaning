package data.cleaning.core.utils.search;

import java.util.ArrayList;
import java.util.List;

import data.cleaning.core.service.repair.impl.Recommendation;

public class Choice {
	private List<Recommendation> recs;
	private double dist;

	public Choice() {
		this.recs = new ArrayList<>();
	}

	public List<Recommendation> getRecs() {
		return recs;
	}

	public void setRecs(List<Recommendation> recs) {
		this.recs = recs;
	}

	public double getDist() {
		return dist;
	}

	public void setDist(double dist) {
		this.dist = dist;
	}

	@Override
	public String toString() {
		return "Choice [recs=" + recs + ", dist=" + dist + "]";
	}

}
