package data.cleaning.core.utils.objectives;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.repair.impl.Candidate;

public class ChangesObjective extends Objective {

	public ChangesObjective(double epsilon, double weight,
			boolean shouldNormalize, Constraint constraint,
			InfoContentTable table) {
		super(epsilon, weight, shouldNormalize, constraint, table);
	}

	@Override
	public double out(Candidate input, TargetDataset tgtDataset,
			MasterDataset mDataset, double maxPvt, double maxInd, long maxSize) {
		return shouldNormalize ? changesNormalized(input, maxSize)
				: changes(input);
	}

	public double changesNormalized(Candidate input, long maxSize) {
		double changesNorm = (double) ((double) changes(input) / (double) maxSize);

//		logger.log(DebugLevel.DEBUG, "Num changes (normalized) : "
//				+ changesNorm);

		return changesNorm;
	}

	public int changes(Candidate input) {

//		logger.log(DebugLevel.DEBUG, "Num changes : "
//				+ input.getRecommendations().size());
		return input.getRecommendations().size();
	}

	@Override
	public String toString() {
		return "ChangesObjective [epsilon=" + epsilon + ", weight=" + weight
				+ ", shouldNormalize=" + shouldNormalize + "]";
	}

}
