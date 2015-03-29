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
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.DebugLevel;
import data.cleaning.core.utils.Pair;

public class CleaningObjective extends Objective {
	final private List<String> ants;
	final private List<String> cons;
	private List<String> antsCons;

	public CleaningObjective(double epsilon, double weight,
			boolean shouldNormalize, Constraint constraint,
			InfoContentTable table) {
		super(epsilon, weight, shouldNormalize, constraint, table);
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

	public double ind(Candidate input, TargetDataset tgtDataset) {
		List<Recommendation> recs = input.getRecommendations();

		List<Record> tgtRecords = tgtDataset.getRecords();
		Multimap<Long, Recommendation> tIdToRec = ArrayListMultimap.create();
		Map<Record, Map<String, String>> oldVals = new HashMap<>();

		for (Recommendation rec : recs) {
			if (rec == null)
				continue;
			tIdToRec.put(rec.gettRid(), rec);
		}

		for (long tId : tIdToRec.keySet()) {

			Record origT = tgtDataset.getRecord(tId);
			Map<String, String> origColsToVal = origT.getColsToVal();
			Collection<Recommendation> toApply = tIdToRec.get(tId);
			Map<String, String> modColsToVal = new HashMap<>();

			for (Recommendation rec : toApply) {
				// Store the old modified value.
				modColsToVal.put(rec.getCol(), origColsToVal.get(rec.getCol()));
				// Apply the new value.
				origT.modifyValForExistingCol(rec.getCol(), rec.getVal());
			}

			oldVals.put(origT, modColsToVal);

		}

		// Ind calculation
		double antsEntropy = 0d;
		double antsAndConsEntropy = 0d;
		double denom = (double) tgtRecords.size();
		Multiset<String> antPToCount = HashMultiset.create();
		Multiset<String> antConsPToCount = HashMultiset.create();
		Map<Long, Pair<String, String>> tIdToAntsAndAntsCons = new HashMap<>();

		// Extremely slow because there are a lot of tgtRecords.
		for (Record record : tgtRecords) {

			StringBuilder antP = new StringBuilder();
			StringBuilder antConsP = new StringBuilder();
			Map<String, String> cv = record.getColsToVal();

			for (String col : ants) {
				antP.append(cv.get(col) + " ");
				antConsP.append(cv.get(col) + " ");
			}

			for (String col : cons) {
				antConsP.append(cv.get(col) + " ");
			}

			String antStr = antP.toString();
			antPToCount.add(antStr);
			String antConsStr = antConsP.toString();
			antConsPToCount.add(antConsStr);
			Pair<String, String> antsAndAntsCons = new Pair<>();
			antsAndAntsCons.setO1(antStr);
			antsAndAntsCons.setO2(antConsStr);
			tIdToAntsAndAntsCons.put(record.getId(), antsAndAntsCons);
		}

		for (String antP : antPToCount.elementSet()) {
			double num = (double) antPToCount.count(antP);
			antsEntropy += (num / denom)
					* (Math.log(denom / num) / Math.log(2d));
		}

		for (String antConsP : antConsPToCount.elementSet()) {
			double num = (double) antConsPToCount.count(antConsP);
			antsAndConsEntropy += (num / denom)
					* (Math.log(denom / num) / Math.log(2d));
		}

		double ind = antsAndConsEntropy - antsEntropy;

		// Rollback changes.
		for (Map.Entry<Record, Map<String, String>> entry : oldVals.entrySet()) {
			Record origT = entry.getKey();

			Map<String, String> modColsToVal = entry.getValue();
			for (Map.Entry<String, String> old : modColsToVal.entrySet()) {
				origT.modifyValForExistingCol(old.getKey(), old.getValue());
			}
		}

		// logger.log(DebugLevel.DEBUG, "Pattern 1 : " + antConsPToCount
		// + ", \nPattern 2 : " + antPToCount);
		logger.log(DebugLevel.DEBUG, "Ind : " + ind);
		return ind;

	}

	public double indNormalized(Candidate input, TargetDataset tgtDataset,
			double maxInd) {
		double indNorm = ind(input, tgtDataset) / maxInd;

		if (indNorm > 1.0d) {
			// logger.log(ProdLevel.PROD, "IND IS NOT NORMALIZED PROPERLY! "
			// + indNorm);
			indNorm = 1.0d;
		}

		// logger.log(DebugLevel.DEBUG, "Ind (normalized) : " + indNorm);

		return indNorm;
	}

	@Override
	public String toString() {
		return "CleaningObjective [epsilon=" + epsilon + ", weight=" + weight
				+ ", shouldNormalize=" + shouldNormalize + "]";
	}

}
