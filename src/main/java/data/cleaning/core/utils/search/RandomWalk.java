package data.cleaning.core.utils.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

public class RandomWalk extends Search {
	final private List<Objective> fns;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;
	final private int numIterations;
	private Map<Objective, RandomWalkStats> objectiveToStats;
	private Map<Objective, Double> objectiveToMin;
	private Map<Objective, Double> objectiveToMax;
	private Map<Objective, Double> objectiveToMean;
	private Map<Objective, Double> objectiveToTotal;

	public RandomWalk(List<Objective> fns, int numIterations,
			InitStrategy strategy, IndNormStrategy indNormStrat) {
		this.fns = fns;
		this.strategy = strategy;
		this.numIterations = numIterations;
		this.indNormStrat = indNormStrat;
		this.objectiveToStats = new HashMap<>();
		this.objectiveToMin = new HashMap<>();
		this.objectiveToMax = new HashMap<>();
		this.objectiveToMean = new HashMap<>();
		this.objectiveToTotal = new HashMap<>();
	}

	/*
	 * More than 1 solution can be found.
	 */
	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table,
			boolean shdReturnInit) {
		double maxInd = calcMaxInd(constraint, tgtDataset.getRecords(),
				indNormStrat);
		double maxPvt = table.getMaxInfoContent();
		PositionalInfo pInfo = calcPositionalInfo(tgtMatches, mDataset,
				constraint);
		Map<Integer, Map<Integer, Choice>> positionToChoices = pInfo
				.getPositionToChoices();

		List<String> cols = constraint.getColsInConstraint();

		int sigSize = positionToChoices.keySet().size() * cols.size();
		long recSize = sigSize;

		if (sigSize <= 0)
			return null;

		int numBitFlipNeighb = sigSize;

		int numChoiceNeighb = 0;

		for (Map<Integer, Choice> choice : positionToChoices.values()) {
			numChoiceNeighb += choice.size() - 1;
		}

		logger.log(ProdLevel.PROD, "\n\nNum neighbours : "
				+ (numBitFlipNeighb + numChoiceNeighb));

		Candidate currentSoln = getInitialSoln(strategy, sigSize,
				positionToChoices, pInfo.getPositionToExactMatch(),
				pInfo.getTidToPosition());

		List<Recommendation> currentSolnRecs = currentSoln.getRecommendations();

		if (currentSoln == null || currentSolnRecs == null
				|| currentSolnRecs.isEmpty())
			return null;

		for (Objective fn : fns) {
			fn.out(currentSoln, tgtDataset, mDataset, maxPvt, maxInd, recSize);
		}

		for (int i = 0; i < numIterations; i++) {

			Candidate randNeighb = getRandNeighb(numBitFlipNeighb,
					numChoiceNeighb, currentSoln, positionToChoices);

			if (randNeighb == null)
				continue;

			List<Recommendation> sRecs = randNeighb.getRecommendations();

			if (sRecs.isEmpty())
				continue;

			for (Objective fn : fns) {
				double objOut = fn.out(randNeighb, tgtDataset, mDataset,
						maxPvt, maxInd, recSize);

				if (!objectiveToMin.containsKey(fn)) {
					objectiveToMin.put(fn, Double.MAX_VALUE);
				}

				if (!objectiveToMax.containsKey(fn)) {
					objectiveToMax.put(fn, Double.MIN_VALUE);
				}

				if (!objectiveToTotal.containsKey(fn)) {
					objectiveToTotal.put(fn, 0d);
				}

				double min = objectiveToMin.get(fn);
				double max = objectiveToMax.get(fn);

				if (objOut < min) {
					objectiveToMin.put(fn, objOut);
				}

				if (objOut > max) {
					objectiveToMax.put(fn, objOut);
				}

				objectiveToTotal.put(fn, objectiveToTotal.get(fn) + objOut);
			}

			currentSoln = randNeighb;

		}

		for (Objective fn : fns) {
			if (objectiveToTotal.containsKey(fn))
				objectiveToMean.put(fn, objectiveToTotal.get(fn)
						/ (double) numIterations);
		}

		return null;
	}

	public Map<Objective, RandomWalkStats> getObjectiveToStats() {
		for (Objective fn : fns) {
			RandomWalkStats rStats = new RandomWalkStats();
			if (objectiveToMax.containsKey(fn))
				rStats.setMax(objectiveToMax.get(fn));
			if (objectiveToMin.containsKey(fn))
				rStats.setMin(objectiveToMin.get(fn));
			if (objectiveToMean.containsKey(fn))
				rStats.setMean(objectiveToMean.get(fn));
			objectiveToStats.put(fn, rStats);
		}

		return objectiveToStats;
	}

	public void setObjectiveToStats(
			Map<Objective, RandomWalkStats> objectiveToStats) {
		this.objectiveToStats = objectiveToStats;
	}

}
