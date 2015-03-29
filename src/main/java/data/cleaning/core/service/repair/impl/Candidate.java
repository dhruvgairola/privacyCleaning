package data.cleaning.core.service.repair.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class Candidate {
	private long id;
	// What is the sequence of bits associated with this candidate?
	private char[] signature;
	// Below is used for hashcode and equals comparison. No nulls exist in this
	// list.
	private List<Recommendation> recommendations;
	// Below is used for neighbour generation. We cannot use "recommendations"
	// for neighbour generation. Likewise, we cannot use "positionToRecs" for
	// hashcode and equals because two different "positionToRecs" can have the
	// exact same recommendation list e.g.,
	// [(t4,m4,LName,Barker), null, (t5,m3,FName,Bianc), (t5,m3,LName,Barker),
	// null, (t7,m4,LName,Barker)], Sign:[0, 0, 1, 1, 1, 1, 1, 0, 1] and
	// [(t4,m3,LName,Barker), null, (t5,m3,FName,Bianc), (t5,m3,LName,Barker),
	// (t7,m4,LName,Barker)], Sign:[0, 0, 1, 1, 1, 1, 0, 0, 1] are different
	// "positionToRecs" as they have different signs. But both are essentially
	// the same recommendations.
	private Map<Integer, List<Recommendation>> positionToRecs;
	private Map<Integer, Integer> positionToChoiceNum;
	private Map<Long, Integer> tIdToPosition;

	// Diff information- how is this candidate different from the current simul
	// annealing solution (NOT current neighb being evaluated, which could end
	// up being discarded)? Needed in order to speed up computation of
	// objectives.
	private boolean isNeighbour;
	private NeighbourType neighbType;
	private List<Recommendation> added;
	private List<Recommendation> removed;

	// Custom flag for ind objective optimization.
	private boolean shdReverseInd;

	// Optimization for eps lex.
	private double pvtOut;
	private double indOut;
	private double changesOut;

	// Can be any random debugging info.
	private String debugging;
	private static final Logger logger = Logger.getLogger(Candidate.class);

	public Candidate() {
		this.positionToChoiceNum = new LinkedHashMap<>();
		this.positionToRecs = new LinkedHashMap<>();
		this.recommendations = new ArrayList<>();
		this.added = new ArrayList<>();
		this.removed = new ArrayList<>();
		this.tIdToPosition = new HashMap<>();
	}

	public Map<Integer, Integer> getPositionToChoiceNumCopy() {
		return new LinkedHashMap<>(positionToChoiceNum);
	}

	public void setPositionToChoiceNum(Map<Integer, Integer> positionToChoiceNum) {
		this.positionToChoiceNum = positionToChoiceNum;
	}

	// We do this because "recommendations" obj is completely dependent on
	// "positionToRecs" obj. We don't want a client to change "positionToRecs"
	// elements and then have to call calcRecommendations() again (this might
	// introduce annoying bugs). We also don't want "recommendations" to be
	// computed each time getRecommendations is called because "recommendations"
	// are used for hashcode and equals, so we want to lazy load them for
	// efficiency.
	public Map<Integer, List<Recommendation>> getPositionToRecsCopy() {
		return new LinkedHashMap<>(positionToRecs);
	}

	public void setPositionToRecs(
			Map<Integer, List<Recommendation>> positionToRecs) {
		this.positionToRecs = positionToRecs;
		calcRecommendations();
	}

	private List<Recommendation> calcRecommendations() {
		recommendations = new ArrayList<>();

		for (List<Recommendation> rs : positionToRecs.values()) {
			for (Recommendation r : rs) {
				if (r != null)
					recommendations.add(r);
			}

		}

		return recommendations;
	}

	public List<Recommendation> getRecommendations() {
		if (recommendations == null || recommendations.isEmpty()) {
			// logger.log(ProdLevel.PROD, "Don't use cache");
			calcRecommendations();
		} else {
			// logger.log(ProdLevel.PROD, "Use cache");
		}

		return recommendations;
	}

	public char[] getSignature() {
		return signature;
	}

	public char[] getSignatureCopy() {
		return Arrays.copyOf(signature, signature.length);
	}

	public void setSignature(char[] signature) {
		this.signature = signature;
	}

	public boolean isNeighbour() {
		return isNeighbour;
	}

	public void setNeighbour(boolean isNeighbour) {
		this.isNeighbour = isNeighbour;
	}

	public List<Recommendation> getAdded() {
		return added;
	}

	public void setAdded(List<Recommendation> added) {
		this.added = added;
	}

	public List<Recommendation> getRemoved() {
		return removed;
	}

	public void setRemoved(List<Recommendation> removed) {
		this.removed = removed;
	}

	public NeighbourType getNeighbType() {
		return neighbType;
	}

	public void setNeighbType(NeighbourType neighbType) {
		this.neighbType = neighbType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((recommendations == null) ? 0 : recommendations.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Candidate other = (Candidate) obj;
		if (recommendations == null) {
			if (other.recommendations != null)
				return false;
		} else if (!recommendations.equals(other.recommendations))
			return false;
		return true;
	}

	public String getDebugging() {
		return debugging;
	}

	public void setDebugging(String debugging) {
		this.debugging = debugging;
	}

	@Override
	public String toString() {
		// Use the first one for debugging and second for printing to file.
		// return "Recommendations [positionToRecs=" + positionToRecs + "]";
		return "Recommendations [recommendations=" + getRecommendations() + "]";
	}

	public Map<Long, Integer> gettIdToPosition() {
		return tIdToPosition;
	}

	public void settIdToPosition(Map<Long, Integer> tIdToPosition) {
		this.tIdToPosition = tIdToPosition;
	}

	public boolean isShdReverseInd() {
		return shdReverseInd;
	}

	public void setShdReverseInd(boolean shdReverseInd) {
		this.shdReverseInd = shdReverseInd;
	}

	public double getPvtOut() {
		return pvtOut;
	}

	public void setPvtOut(double pvtOut) {
		this.pvtOut = pvtOut;
	}

	public double getIndOut() {
		return indOut;
	}

	public void setIndOut(double indOut) {
		this.indOut = indOut;
	}

	public double getChangesOut() {
		return changesOut;
	}

	public void setChangesOut(double changesOut) {
		this.changesOut = changesOut;
	}

	public Multimap<Long, Recommendation> getTidToRecs(Set<Long> tIds) {
		Multimap<Long, Recommendation> tidToRecs = ArrayListMultimap.create();
		for (long tId : tIds) {
			int position = tIdToPosition.get(tId);
			List<Recommendation> recs = positionToRecs.get(position);
			tidToRecs.putAll(tId, recs);
		}

		return tidToRecs;
	}

}
