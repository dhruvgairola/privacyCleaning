package data.cleaning.core.utils.objectives;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.BiMap;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.Recommendation;

public class PrivacyObjective extends Objective {

	public PrivacyObjective(double epsilon, double weight,
			boolean shouldNormalize, Constraint constraint,
			InfoContentTable table) {
		super(epsilon, weight, shouldNormalize, constraint, table);
	}

	@Override
	public double out(Candidate input, TargetDataset tgtDataset,
			MasterDataset mDataset, double maxPvt, double maxInd, long maxSize) {
		return shouldNormalize ? pvtLossNormalized(input, tgtDataset, maxPvt)
				: pvtLoss(input);
	}

	public double pvtLossNormalized(Candidate input, TargetDataset tgtDataset,
			double maxPvt) {
		double pvtNorm = pvtLoss(input) / maxPvt;

		// logger.log(DebugLevel.DEBUG, "Pvt loss (normalized) : " + pvtNorm);
		return pvtNorm;
	}

	public double pvtLoss(Candidate input) {

		double[][] data = table.getData();
		BiMap<String, Integer> colNameToId = table.getColIdToName().inverse();
		double pvtLoss = calcPvtLoss(input.getRecommendations(), data,
				colNameToId);
		// logger.log(DebugLevel.DEBUG, "Pvt loss : " + pvtLoss);
		return pvtLoss;

	}

	private double calcPvtLoss(List<Recommendation> recs, double[][] data,
			BiMap<String, Integer> colNameToId) {
		if (recs == null || recs.isEmpty())
			return 0d;

		double pvtLoss = 0d;

		Set<String> pastCells = new HashSet<>();

		for (Recommendation rec : recs) {
			if (rec == null)
				continue;

			int rowId = (int) rec.getmRid() - 1;

			int colId = colNameToId.get(rec.getCol());

			if (!pastCells.contains(rowId + "," + colId)) {
				pvtLoss += data[rowId][colId];
			}

			pastCells.add(rowId + "," + colId);

		}

		return pvtLoss;
	}

	@Override
	public String toString() {
		return "PvtObjective [epsilon=" + epsilon + ", weight=" + weight
				+ ", shouldNormalize=" + shouldNormalize + "]";
	}

}
