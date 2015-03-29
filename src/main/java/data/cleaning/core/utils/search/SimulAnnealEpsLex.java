package data.cleaning.core.utils.search;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.objectives.CustomCleaningObjective;
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

/**
 * Lexicographic, where each objective has an associated threshold.
 * 
 * @author dhruvgairola
 *
 */
public class SimulAnnealEpsLex extends Search {
	final double initTemperature;
	final double finalTemperature;
	// Used in temp decay function.
	final double alpha;
	final private List<Objective> fns;
	final private double bestEnergy;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;

	public SimulAnnealEpsLex(List<Objective> fns, double initTemp,
			double finalTemperature, double alpha, double bestEnergy,
			InitStrategy strategy, IndNormStrategy indNormStrat) {
		this.initTemperature = initTemp;
		this.finalTemperature = finalTemperature;
		this.alpha = alpha;
		this.bestEnergy = bestEnergy;
		this.fns = fns;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table,
			boolean shdReturnInit) {
		double temperature = initTemperature;

		Set<Candidate> solns = new HashSet<>();
		double maxInd = calcMaxInd(constraint, tgtDataset.getRecords(),
				indNormStrat);
		double maxPvt = table.getMaxInfoContent();

		PositionalInfo pInfo = calcPositionalInfo(tgtMatches, mDataset,
				constraint);
		Map<Integer, Map<Integer, Choice>> positionToChoices = pInfo
				.getPositionToChoices();
		List<String> cols = constraint.getColsInConstraint();

		long recSize = positionToChoices.keySet().size() * cols.size();

		if (recSize <= 0)
			return null;

		for (int i = 0; i < fns.size(); i++) {
			Objective obj = fns.get(i);

			if (i == 0) {
				SimulAnnealEpsThreshold h = new SimulAnnealEpsThreshold(obj,
						fns.subList(1, fns.size()), temperature,
						finalTemperature, alpha, bestEnergy, strategy,
						indNormStrat);
				Set<Candidate> optSolns = h.calcOptimalSolns(constraint,
						tgtMatches, tgtDataset, mDataset, table, shdReturnInit);
				if (optSolns == null || optSolns.isEmpty())
					return null;

				if (optSolns.size() > 10) {
					int added = 0;
					for (Candidate optSoln : optSolns) {
						solns.add(optSoln);
						if (added == 9)
							break;
						added++;
					}
				} else {
					solns.addAll(optSolns);
				}

				// logger.log(ProdLevel.PROD, "\nMinimizing : " + fns.get(i));
				// logger.log(ProdLevel.PROD, "Optimal solns size : " +
				// optSolns.size());

			} else if (obj instanceof CustomCleaningObjective) {
				// TODO: Somewhat hacky.
				solns = minimizeInd(constraint, solns, obj, tgtDataset,
						mDataset, table, maxPvt, maxInd, recSize);
			}

		}

		return solns;
	}

	private Set<Candidate> minimizeInd(Constraint constraint,
			Set<Candidate> toPrune, Objective objective,
			TargetDataset tgtDataset, MasterDataset mDataset,
			InfoContentTable table, double maxPvt, double maxInd, long recSize) {
		Set<Candidate> filtered = new HashSet<>();
		double minSoln = Double.MAX_VALUE;

		// logger.log(ProdLevel.DEBUG, "\nMinimizing : " + objective);

		for (Candidate can : toPrune) {
			double out = can.getIndOut();

			// can.setDebugging(can.getDebugging() + ", "
			// + objective.getClass().getSimpleName() + " : " + out
			// + " \n");

			if (Math.abs(minSoln - out) <= Config.FLOAT_EQUALIY_EPSILON) {
				filtered.add(can);
			} else if (out < minSoln) {
				minSoln = out;
				filtered.removeAll(filtered);
				filtered.add(can);
			} else {
				// logger.log(ProdLevel.DEBUG, "Filtered out soln : " + can);
			}
		}

		return filtered;
	}

	public List<Objective> getObjectives() {
		return fns;
	}

}
