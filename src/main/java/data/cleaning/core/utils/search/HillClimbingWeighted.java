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
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

/**
 * No sideways moves allowed.
 * 
 * @author dhruvgairola
 *
 */
public class HillClimbingWeighted extends Search {
	final private List<Objective> weightedFns;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;

	public HillClimbingWeighted(List<Objective> weightedFns,
			InitStrategy strategy, IndNormStrategy indNormStrat) {
		this.weightedFns = weightedFns;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

	/*
	 * Only 1 optimal solution is ever returned since no sideways moves are
	 * allowed.
	 */
	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table,
			boolean shdReturnInit) {
		Set<Candidate> solns = new HashSet<>();
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

		Candidate nextSoln = null;

		Candidate currentSoln = getInitialSoln(strategy, sigSize,
				positionToChoices, pInfo.getPositionToExactMatch(),
				pInfo.getTidToPosition());

		if (shdReturnInit) {
			solns.add(currentSoln);
			return solns;
		}

		double nextEval = Double.MAX_VALUE;

		while (true) {

			nextEval = Double.MAX_VALUE;
			nextSoln = null;

			Set<Candidate> neighbs = getNeighbs(currentSoln, positionToChoices);

			// TODO
			// copyOutputInfo(currentSoln, randNeighb);

			for (Candidate neighb : neighbs) {
				List<Recommendation> sRecs = neighb.getRecommendations();
				if (sRecs.isEmpty())
					continue;

				logger.log(ProdLevel.PROD, "Solution:" + neighb);
				double fnOut = 0d;

				for (Objective weightedFn : weightedFns) {
					fnOut += weightedFn.out(neighb, tgtDataset, mDataset,
							maxPvt, maxInd, recSize) * weightedFn.getWeight();
				}

				logger.log(ProdLevel.PROD, "Out : " + fnOut + "\n");

				if (fnOut < nextEval) {
					nextSoln = neighb;
					nextEval = fnOut;
				}
			}

			logger.log(ProdLevel.PROD, "Back to current Solution:"
					+ currentSoln);
			double currentFnOut = 0d;

			for (Objective weightedFn : weightedFns) {
				currentFnOut += weightedFn.out(currentSoln, tgtDataset,
						mDataset, maxPvt, maxInd, recSize)
						* weightedFn.getWeight();
			}

			logger.log(ProdLevel.PROD, "Out : " + currentFnOut + "\n");
			if (nextEval >= currentFnOut) {

				logger.log(ProdLevel.PROD,
						"Current soln is the best. Terminate algo.\n\n");
				break;
			}

			currentSoln = nextSoln;
			logger.log(ProdLevel.PROD, "Better soln was found : " + nextSoln
					+ "\n\n");

		}

		solns.add(currentSoln);

		return solns;
	}

}
