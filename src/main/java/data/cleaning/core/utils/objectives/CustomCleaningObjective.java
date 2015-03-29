package data.cleaning.core.utils.objectives;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.DatasetStats;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.DebugLevel;
import data.cleaning.core.utils.Pair;

/**
 * This class is used for simulated annealing expts. Do not use as standalone
 * objective because class this maintains a history.
 * 
 * @author dhruvgairola
 *
 */
public class CustomCleaningObjective extends Objective {
	final private List<String> ants;
	final private List<String> cons;
	private List<String> antsCons;
	private Constraint constraint;

	// Current statistics.
	private Multiset<String> antPToCount;
	private Multiset<String> antConsPToCount;
	private Map<Long, Pair<String, String>> tIdToAntsAndAntsCons;
	private double antsEntropy;
	private double antsAndConsEntropy;
	private double denom;

	// Maintain a history of 1 step.
	private double prevAntsEntropy;
	private double prevAntsAndConsEntropy;
	private Multiset<String> antConsPToCountRemoved;
	private Multiset<String> antConsPToCountAdded;
	private Multiset<String> antPToCountRemoved;
	private Multiset<String> antPToCountAdded;
	private Map<Long, Pair<String, String>> tIdToAntsAndAntsConsRemoved;

	public CustomCleaningObjective(double epsilon, double weight,
			boolean shouldNormalize, Constraint constraint,
			InfoContentTable table) {
		super(epsilon, weight, shouldNormalize, constraint, table);
		this.constraint = constraint;
		this.ants = constraint.getAntecedentCols();
		this.cons = constraint.getConsequentCols();
		this.antsCons = new ArrayList<>();
		this.antsCons.addAll(ants);
		this.antsCons.addAll(cons);
	}

	@Override
	public double getEpsilon() {
		return epsilon;
	}

	@Override
	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	@Override
	public double getWeight() {
		return weight;
	}

	@Override
	public void setWeight(double weight) {
		this.weight = weight;
	}

	@Override
	public double out(Candidate input, TargetDataset tgtDataset,
			MasterDataset mDataset, double maxPvt, double maxInd, long maxSize) {
		return shouldNormalize ? indNormalized(input, tgtDataset, maxInd)
				: ind(input, tgtDataset);
	}

	// Optimized, but difficult to debug.
	public double ind(Candidate input, TargetDataset tgtDataset) {

		if (input.isShdReverseInd()) {
			reverseStats(input);
		}

		// Only the first input is not a neighb.
		if (!input.isNeighbour()) {
			// Update statistics. Below is done once for every simul anneal
			// expt.
			DatasetStats dStats = tgtDataset.getDatasetStats();
			antConsPToCount = dStats.getAntsConsPToCountCopy(constraint);
			antPToCount = dStats.getAntPToCountCopy(constraint);
			antsEntropy = dStats.getAntsEntropy(constraint);
			antsAndConsEntropy = dStats.getAntsAndConsEntropy(constraint);
			denom = tgtDataset.getRecords().size();
			tIdToAntsAndAntsCons = dStats
					.gettIdToAntsAndAntsConsCopy(constraint);

			input.setAdded(input.getRecommendations());
			input.setRemoved(new ArrayList<Recommendation>());
		}

		double ind = calcIndUsingStats(input, tgtDataset);

//		logger.log(DebugLevel.DEBUG, "Ind : " + ind);
		return ind;
	}

	/**
	 * Reverse the current statistics by 1 step by using the 1 step history.
	 * This occurs in simulated annealing when a worse solution is discarded.
	 * 
	 * @param input
	 */
	private void reverseStats(Candidate input) {
		input.setShdReverseInd(false);
		antsEntropy = prevAntsEntropy;
		antsAndConsEntropy = prevAntsAndConsEntropy;

		if (antConsPToCountRemoved != null) {
			antConsPToCount.addAll(antConsPToCountRemoved);
		}

		if (antConsPToCountAdded != null) {
			for (Multiset.Entry<String> e : antConsPToCountAdded.entrySet()) {
				antConsPToCount.remove(e.getElement(), e.getCount());
			}
		}

		if (antPToCountRemoved != null) {
			antPToCount.addAll(antPToCountRemoved);
		}

		if (antPToCountAdded != null) {

			for (Multiset.Entry<String> e : antPToCountAdded.entrySet()) {
				antPToCount.remove(e.getElement(), e.getCount());
			}
		}

		if (tIdToAntsAndAntsConsRemoved != null) {
			for (Map.Entry<Long, Pair<String, String>> e : tIdToAntsAndAntsConsRemoved
					.entrySet()) {
				tIdToAntsAndAntsCons.put(e.getKey(), e.getValue());
			}
		}
	}

	/**
	 * Complicated method.
	 * 
	 * @param input
	 * @param tgtDataset
	 * @return
	 */
	private double calcIndUsingStats(Candidate input, TargetDataset tgtDataset) {
		// History
		antConsPToCountRemoved = HashMultiset.create();
		antConsPToCountAdded = HashMultiset.create();
		antPToCountRemoved = HashMultiset.create();
		antPToCountAdded = HashMultiset.create();
		tIdToAntsAndAntsConsRemoved = new HashMap<>();

		// Gather parent info.
		prevAntsEntropy = antsEntropy;
		prevAntsAndConsEntropy = antsAndConsEntropy;

		// What was added or removed.
		List<Recommendation> added = input.getAdded();
		List<Recommendation> removed = input.getRemoved();

		Multimap<Long, Recommendation> tIdToRecAdded = ArrayListMultimap
				.create();
		for (Recommendation a : added) {
			if (a == null)
				continue;
			tIdToRecAdded.put(a.gettRid(), a);
		}

		Multimap<Long, Recommendation> tIdToRecRemoved = ArrayListMultimap
				.create();
		for (Recommendation r : removed) {
			if (r == null)
				continue;
			tIdToRecRemoved.put(r.gettRid(), r);
		}

		Multimap<Long, Recommendation> tidToARecs = input
				.getTidToRecs(tIdToRecAdded.keySet());

		// What was added.
		for (long tId : tIdToRecAdded.keySet()) {
			Record aRec = tgtDataset.getRecord(tId);
			Collection<Recommendation> assocRec = tidToARecs.get(tId);

			Map<String, String> modColsToVal = new HashMap<>();

			// Apply recommendations.
			for (Recommendation assoc : assocRec) {
				if (assoc == null) {
					continue;
				}
				modColsToVal.put(assoc.getCol(),
						aRec.getColsToVal().get(assoc.getCol()));

				aRec.modifyValForExistingCol(assoc.getCol(), assoc.getVal());
			}

			// Remove the prev patterns present in parent.
			if (tIdToAntsAndAntsCons.containsKey(aRec.getId())) {
				Pair<String, String> antsToAntsCons = tIdToAntsAndAntsCons
						.get(aRec.getId());

				double oldCountA = (double) antPToCount.count(antsToAntsCons
						.getO1());
				double oldCountAC = (double) antConsPToCount
						.count(antsToAntsCons.getO2());
				antPToCount.remove(antsToAntsCons.getO1());
				antPToCountRemoved.add(antsToAntsCons.getO1());
				antConsPToCount.remove(antsToAntsCons.getO2());
				antConsPToCountRemoved.add(antsToAntsCons.getO2());
				double newCountA = (double) antPToCount.count(antsToAntsCons
						.getO1());
				double newCountAC = (double) antConsPToCount
						.count(antsToAntsCons.getO2());

				// Adjust entropy accordingly.
				antsEntropy -= (oldCountA / denom)
						* (Math.log(denom / oldCountA) / Math.log(2d));
				if (newCountA > 0) {
					antsEntropy += (newCountA / denom)
							* (Math.log(denom / newCountA) / Math.log(2d));
				}
				antsAndConsEntropy -= (oldCountAC / denom)
						* (Math.log(denom / oldCountAC) / Math.log(2d));
				if (newCountAC > 0) {
					antsAndConsEntropy += (newCountAC / denom)
							* (Math.log(denom / newCountAC) / Math.log(2d));
				}
			}

			// What are the new vals after applying recs?
			String newAntsP = aRec.getRecordStr(ants);
			String newAntsConsP = aRec.getRecordStr(antsCons);
			// Update the pattern store.
			tIdToAntsAndAntsConsRemoved.put(tId, tIdToAntsAndAntsCons.get(tId));
			Pair<String, String> aac = new Pair<>();
			aac.setO1(newAntsP);
			aac.setO2(newAntsConsP);
			tIdToAntsAndAntsCons.put(tId, aac);

			// Add the new patterns.
			double oldCountA = (double) antPToCount.count(aac.getO1());
			double oldCountAC = (double) antConsPToCount.count(aac.getO2());

			antPToCount.add(aac.getO1());
			antPToCountAdded.add(aac.getO1());
			antConsPToCount.add(aac.getO2());
			antConsPToCountAdded.add(aac.getO2());
			double newCountA = (double) antPToCount.count(aac.getO1());
			double newCountAC = (double) antConsPToCount.count(aac.getO2());

			// Adjust entropy accordingly.
			if (oldCountA > 0) {
				antsEntropy -= (oldCountA / denom)
						* (Math.log(denom / oldCountA) / Math.log(2d));
			}

			if (oldCountAC > 0) {
				antsAndConsEntropy -= (oldCountAC / denom)
						* (Math.log(denom / oldCountAC) / Math.log(2d));

			}

			antsEntropy += (newCountA / denom)
					* (Math.log(denom / newCountA) / Math.log(2d));
			antsAndConsEntropy += (newCountAC / denom)
					* (Math.log(denom / newCountAC) / Math.log(2d));

			// Rollback changes.
			for (Map.Entry<String, String> e : modColsToVal.entrySet()) {
				aRec.modifyValForExistingCol(e.getKey(), e.getValue());
			}

		}

		// Need to do this because recs were removed. But we need to know what
		// the old value of the removed column is. Is the old value to original
		// value of the column or is it some value from the recommendation set?
		// Hence, we need to call getTidToRecs in case it is from the
		// recommendation set.
		Multimap<Long, Recommendation> tidToRRecs = input
				.getTidToRecs(tIdToRecRemoved.keySet());

		// What was removed.
		for (long tId : tIdToRecRemoved.keySet()) {
			Record aRec = tgtDataset.getRecord(tId);
			Collection<Recommendation> assocRec = tidToRRecs.get(tId);
			Map<String, String> modColsToVal = new HashMap<>();

			// Apply recommendations.
			for (Recommendation assoc : assocRec) {
				if (assoc == null) {
					continue;
				}

				modColsToVal.put(assoc.getCol(),
						aRec.getColsToVal().get(assoc.getCol()));

				aRec.modifyValForExistingCol(assoc.getCol(), assoc.getVal());
			}

			// Remove the prev patterns present in parent.
			if (tIdToAntsAndAntsCons.containsKey(aRec.getId())) {
				Pair<String, String> antsToAntsCons = tIdToAntsAndAntsCons
						.get(aRec.getId());
				double oldCountA = (double) antPToCount.count(antsToAntsCons
						.getO1());
				double oldCountAC = (double) antConsPToCount
						.count(antsToAntsCons.getO2());
				antPToCount.remove(antsToAntsCons.getO1());
				antPToCountRemoved.add(antsToAntsCons.getO1());
				antConsPToCount.remove(antsToAntsCons.getO2());
				antConsPToCountRemoved.add(antsToAntsCons.getO2());
				double newCountA = (double) antPToCount.count(antsToAntsCons
						.getO1());
				double newCountAC = (double) antConsPToCount
						.count(antsToAntsCons.getO2());

				// Adjust entropy accordingly.
				antsEntropy -= (oldCountA / denom)
						* (Math.log(denom / oldCountA) / Math.log(2d));
				if (newCountA > 0) {
					antsEntropy += (newCountA / denom)
							* (Math.log(denom / newCountA) / Math.log(2d));
				}
				antsAndConsEntropy -= (oldCountAC / denom)
						* (Math.log(denom / oldCountAC) / Math.log(2d));
				if (newCountAC > 0) {
					antsAndConsEntropy += (newCountAC / denom)
							* (Math.log(denom / newCountAC) / Math.log(2d));
				}

			}

			// What are the new vals after applying recs?
			String newAntsP = aRec.getRecordStr(ants);
			String newAntsConsP = aRec.getRecordStr(antsCons);
			// Update the pattern store.

			// HACK: The way the diff (added) mechanism works means that
			// tIdToAntsAndAntsCons will be replaced with the newly added value.
			// Hence, if we add this value to tIdToAntsAndAntsConsRemoved now,
			// then the contents of tIdToAntsAndAntsConsRemoved will be
			// inaccurate because it has been replaced with the new value (in
			// line #[search for prev occurance of
			// tIdToAntsAndAntsConsRemoved]).
			if (!tIdToAntsAndAntsConsRemoved.containsKey(tId))
				tIdToAntsAndAntsConsRemoved.put(tId,
						tIdToAntsAndAntsCons.get(tId));
			Pair<String, String> aac = new Pair<>();
			aac.setO1(newAntsP);
			aac.setO2(newAntsConsP);
			tIdToAntsAndAntsCons.put(tId, aac);

			// Add the new patterns.
			double oldCountA = (double) antPToCount.count(aac.getO1());
			double oldCountAC = (double) antConsPToCount.count(aac.getO2());
			antPToCount.add(aac.getO1());
			antPToCountAdded.add(aac.getO1());
			antConsPToCount.add(aac.getO2());
			antConsPToCountAdded.add(aac.getO2());
			double newCountA = (double) antPToCount.count(aac.getO1());
			double newCountAC = (double) antConsPToCount.count(aac.getO2());

			// Adjust entropy accordingly.
			if (oldCountA > 0) {
				antsEntropy -= (oldCountA / denom)
						* (Math.log(denom / oldCountA) / Math.log(2d));
			}

			if (oldCountAC > 0) {
				antsAndConsEntropy -= (oldCountAC / denom)
						* (Math.log(denom / oldCountAC) / Math.log(2d));
			}

			antsEntropy += (newCountA / denom)
					* (Math.log(denom / newCountA) / Math.log(2d));
			antsAndConsEntropy += (newCountAC / denom)
					* (Math.log(denom / newCountAC) / Math.log(2d));

			// Rollback changes.
			for (Map.Entry<String, String> e : modColsToVal.entrySet()) {
				aRec.modifyValForExistingCol(e.getKey(), e.getValue());
			}

		}

		// logger.log(DebugLevel.DEBUG, "Pattern 1 : " + antConsPToCount
		// + ", \nPattern 2 : " + antPToCount);
		return antsAndConsEntropy - antsEntropy;
	}

	public double indNormalized(Candidate input, TargetDataset tgtDataset,
			double maxInd) {
		double indNorm = ind(input, tgtDataset) / maxInd;

		if (indNorm > 1.0d) {
			// logger.log(ProdLevel.PROD, "IND IS NOT NORMALIZED PROPERLY! "
			// + indNorm);
			indNorm = 1.0d;
		}

		// logger.log(ProdLevel.PROD, "Ind (normalized) : " + indNorm);

		return indNorm;
	}

	@Override
	public String toString() {
		return "CustomCleaningObjective [epsilon=" + epsilon + ", weight="
				+ weight + ", shouldNormalize=" + shouldNormalize + "]";
	}

}
