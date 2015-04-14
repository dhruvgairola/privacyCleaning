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
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.DebugLevel;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.objectives.ChangesObjective;
import data.cleaning.core.utils.objectives.CustomCleaningObjective;
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

public class SimulAnnealEpsThreshold extends Search {

	final double initTemperature;
	final double finalTemperature;
	// Used in temp decay function.
	final double alpha;
	final private Objective boundedFn;
	final private List<Objective> otherFns;
	final private double bestEnergy;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;
	private double epsilon;

	public SimulAnnealEpsThreshold(Objective boundedFn,
			List<Objective> otherFns, double initTemp, double finalTemperature,
			double alpha, double bestEnergy, InitStrategy strategy,
			IndNormStrategy indNormStrat) {
		this.initTemperature = initTemp;
		this.finalTemperature = finalTemperature;
		this.alpha = alpha;
		this.boundedFn = boundedFn;
		this.otherFns = otherFns;
		this.bestEnergy = bestEnergy;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table) {

		int numIter = (int) Math.ceil(Math.log(finalTemperature
				/ initTemperature)
				/ Math.log(alpha));

		logger.log(ProdLevel.PROD, "\n\nTheoretically max iterations : "
				+ numIter);

		double temperature = initTemperature;
		double maxInd = calcMaxInd(constraint, tgtDataset.getRecords(),
				indNormStrat);
		double maxPvt = table.getMaxInfoContent();

		Set<Candidate> solns = new HashSet<>();
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

		if (currentSoln == null || currentSoln.getRecommendations() == null
				|| currentSoln.getRecommendations().isEmpty())
			return null;

		int numBitFlipNeighb = sigSize;

		int numChoiceNeighb = 0;

		for (Map<Integer, Choice> choice : positionToChoices.values()) {
			numChoiceNeighb += choice.size() - 1;
		}

		double currentFnOut = boundedFn.out(currentSoln, tgtDataset, mDataset,
				maxPvt, maxInd, recSize);
		currentSoln.setPvtOut(currentFnOut);

		// Flexible epsilon.
		epsilon = boundedFn.getEpsilon();

		for (Objective otherFn : otherFns) {
			double bOut = otherFn.out(currentSoln, tgtDataset, mDataset,
					maxPvt, maxInd, recSize);

			if (otherFn instanceof CustomCleaningObjective) {
				currentSoln.setIndOut(bOut);
			} else if (otherFn instanceof ChangesObjective) {
				currentSoln.setChangesOut(bOut);
			}
		}

		// How many worse solutions are accepted throughout this process?
		int accepted = 0;
		int allWorse = 0;

		int iter = 0;
		boolean currentSatisfiesBound = false;
		while (temperature > finalTemperature) {
			iter++;

			if (currentFnOut <= bestEnergy && currentSatisfiesBound) {
				logger.log(ProdLevel.PROD, "Best energy (" + bestEnergy
						+ ") was reached.");
				return solns;
			}

			// Steps per temp in order to stabilize the solutions.
			for (int step = 0; step < Config.STEPS_PER_TEMP; step++) {
				Candidate randNeighb = getRandNeighb(numBitFlipNeighb,
						numChoiceNeighb, currentSoln, positionToChoices);

				if (randNeighb == null
						|| randNeighb.getRecommendations().isEmpty())
					break;

				List<Recommendation> sRecs = randNeighb.getRecommendations();

				if (countCache.containsKey(sRecs)) {
					int countNeighb = countCache.get(sRecs);

					if (countNeighb + 1 > Config.SA_REPEAT_NEIGHB_THRESHOLD) {
						logger.log(ProdLevel.PROD, "Same neighb " + randNeighb
								+ " was seen "
								+ Config.SA_REPEAT_NEIGHB_THRESHOLD
								+ " times. Terminating.");
						return solns;
					} else {
						countCache.put(sRecs, countNeighb + 1);
					}
				} else {
					countCache.put(sRecs, 1);
				}

				// logger.log(DebugLevel.DEBUG, "\nNeighb:" + sRecs
				// + ", \nIteration : " + iter);

				double fnOut = boundedFn.out(randNeighb, tgtDataset, mDataset,
						maxPvt, maxInd, recSize);
				randNeighb.setPvtOut(fnOut);

				// Optimization: calc the outputs of the other objectives here.
				for (Objective otherFn : otherFns) {
					double bOut = otherFn.out(randNeighb, tgtDataset, mDataset,
							maxPvt, maxInd, recSize);

					if (otherFn instanceof CustomCleaningObjective) {
						randNeighb.setIndOut(bOut);
					} else if (otherFn instanceof ChangesObjective) {
						randNeighb.setChangesOut(bOut);
					}
				}

				// randNeighb.setDebugging(boundedFn.getClass().getSimpleName()
				// + " : " + fnOut + " \nIteration : " + iter);

				double delta = currentFnOut - fnOut;

				boolean satisfiesBound = fnOut <= epsilon;

				if (satisfiesBound) {
					currentFnOut = fnOut;
					currentSoln = randNeighb;
					currentSatisfiesBound = satisfiesBound;

					solns.add(currentSoln);
					logger.log(DebugLevel.DEBUG, "Neighb is accepted.");

				} else {

					allWorse++;
					double randomTest = rand.nextDouble();
					double acceptanceProb = Math.exp(delta / temperature);
					// if exp ( dE/T ) > k then accept worse solution
					if (acceptanceProb > randomTest) {
						// increase "worse solutions accepted" counter
						accepted++;

						currentFnOut = fnOut;
						currentSoln = randNeighb;
						currentSatisfiesBound = satisfiesBound;

					} else {
						currentSoln.setShdReverseInd(true);
					}
				}
			}
			temperature = temperature * alpha;
		}

		if (solns.isEmpty()) {
			solns.add(currentSoln);
		}

		logger.log(ProdLevel.PROD, "\n\nTot worse solns : " + allWorse
				+ ", Accepted worse solns : " + accepted);
		return solns;
	}
}
