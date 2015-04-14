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

public class HillClimbingEps extends Search {
	final private Objective fn;
	final private Set<Objective> boundedFns;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;

	public HillClimbingEps(Objective fn, Set<Objective> boundedFns,
			InitStrategy strategy, IndNormStrategy indNormStrat) {
		this.fn = fn;
		this.boundedFns = boundedFns;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

	/*
	 * Sideways moves are allowed.
	 */
	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table) {
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

		Candidate currentSoln = getInitialSoln(strategy, sigSize,
				positionToChoices, pInfo.getPositionToExactMatch(),
				pInfo.getTidToPosition());

		Candidate nextSoln = null;
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

				double fnout = fn.out(neighb, tgtDataset, mDataset, maxPvt,
						maxInd, recSize);

				logger.log(ProdLevel.PROD, "\nSolution:" + neighb);

				boolean satisfiesBound = true;

				for (Objective bounded : boundedFns) {
					double bOut = bounded.out(neighb, tgtDataset, mDataset,
							maxPvt, maxInd, recSize);
					satisfiesBound = satisfiesBound
							&& bOut <= bounded.getEpsilon();
				}

				if (fnout < nextEval && satisfiesBound) {
					nextSoln = neighb;
					nextEval = fnout;
				}
			}

			double currentfnout = fn.out(currentSoln, tgtDataset, mDataset,
					maxPvt, maxInd, recSize);

			if (nextEval >= currentfnout) {
				break;
			}

			currentSoln = nextSoln;
		}

		solns.add(currentSoln);

		return solns;
	}

}
