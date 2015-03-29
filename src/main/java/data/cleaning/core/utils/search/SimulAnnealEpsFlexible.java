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
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.objectives.CleaningObjective;
import data.cleaning.core.utils.objectives.CustomCleaningObjective;
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

/**
 * This is different from SimulAnnealEps where the ind, pvt and changes
 * functions are decided based on the initial ind, pvt and changes output of the
 * initial solution. This is better than SimulAnnealEps because it you don't
 * have to think of an appropriate threshold.
 * 
 * @author dhruvgairola
 *
 */
public class SimulAnnealEpsFlexible extends Search {

	final double initTemperature;
	final double finalTemperature;
	// Used in temp decay function.
	final double alpha;
	final private Objective fn;
	final private Set<Objective> boundedFns;
	final private double bestEnergy;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;
	private double epsilonCleaning;

	public SimulAnnealEpsFlexible(Objective fn, Set<Objective> boundedFns,
			double initTemp, double finalTemperature, double alpha,
			double bestEnergy, InitStrategy strategy,
			IndNormStrategy indNormStrat) {
		this.initTemperature = initTemp;
		this.finalTemperature = finalTemperature;
		this.alpha = alpha;
		this.fn = fn;
		this.boundedFns = boundedFns;
		this.bestEnergy = bestEnergy;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table,
			boolean shdReturnInit) {

		int numIter = (int) Math.ceil(Math.log(finalTemperature
				/ initTemperature)
				/ Math.log(alpha));

		logger.log(ProdLevel.PROD, "\n\nTheoretically max iterations : "
				+ numIter);

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

		int sigSize = positionToChoices.keySet().size() * cols.size();
		long recSize = sigSize;

		if (sigSize <= 0)
			return null;

		Candidate currentSoln = getInitialSoln(strategy, sigSize,
				positionToChoices, pInfo.getPositionToExactMatch(),
				pInfo.getTidToPosition());

		if (shdReturnInit) {
			solns.add(currentSoln);
			return solns;
		}

		setFlexiEpsilonInd(currentSoln, tgtDataset, mDataset, maxPvt, maxInd,
				recSize);

		if (currentSoln == null || currentSoln.getRecommendations() == null
				|| currentSoln.getRecommendations().isEmpty())
			return null;

		int numBitFlipNeighb = sigSize;

		int numChoiceNeighb = 0;

		for (Map<Integer, Choice> choice : positionToChoices.values()) {
			numChoiceNeighb += choice.size() - 1;
		}

		double bestFnOut = Double.MAX_VALUE;
		double currentFnOut = fn.out(currentSoln, tgtDataset, mDataset, maxPvt,
				maxInd, recSize);

		logger.log(ProdLevel.PROD, "Out : " + currentFnOut);

		// How many worse solutions are accepted throughout this process?
		int accepted = 0;
		int allWorse = 0;
		boolean currentSatisfiesBound = false;

		solns.add(currentSoln);

		int iter = 0;

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

				double newFnOut = fn.out(randNeighb, tgtDataset, mDataset,
						maxPvt, maxInd, recSize);

				double delta = currentFnOut - newFnOut;

				boolean satisfiesBound = true;

				// logger.log(
				// DebugLevel.DEBUG,
				// "\nNeighb:" + sRecs + "\nAdded : "
				// + randNeighb.getAdded() + "\nRemoved : "
				// + randNeighb.getRemoved() + "\nType : "
				// + randNeighb.getNeighbType());

				// StringBuilder sb = new StringBuilder();
				for (Objective bounded : boundedFns) {
					double bOut = bounded.out(randNeighb, tgtDataset, mDataset,
							maxPvt, maxInd, recSize);

					// sb.append(bounded.getClass().getSimpleName() + " : " +
					// bOut
					// + " \n");
					if (bounded instanceof CustomCleaningObjective
							|| bounded instanceof CleaningObjective) {
						satisfiesBound = satisfiesBound
								&& bOut <= epsilonCleaning;
					} else {
						satisfiesBound = satisfiesBound
								&& bOut <= bounded.getEpsilon();
					}

				}

				// sb.append("Iteration : " + iter);
				// randNeighb.setDebugging(sb.toString());

				// logger.log(DebugLevel.DEBUG, "Out:" + newFnOut
				// + ", Satisfies constraints : " + satisfiesBound);

				if ((Math.abs(delta) <= Config.FLOAT_EQUALIY_EPSILON && satisfiesBound)
						|| (delta > 0 && satisfiesBound)) {
					currentFnOut = newFnOut;
					currentSoln = randNeighb;
					currentSatisfiesBound = satisfiesBound;

					if (Math.abs(delta) <= Config.FLOAT_EQUALIY_EPSILON
							&& satisfiesBound) {
						// logger.log(DebugLevel.DEBUG,
						// "Neighb is equally good. Current solns : "
						// + solns);
					} else {
						// logger.log(DebugLevel.DEBUG,
						// "Neighb is better. Current solns : " + solns);
					}

				} else {

					allWorse++;
					double randomTest = rand.nextDouble();
					double acceptanceProb = Math.exp(delta / temperature);
					// if exp ( dE/T ) > k then accept worse solution
					if (acceptanceProb > randomTest) {
						// increase "worse solutions accepted" counter
						accepted++;
						currentFnOut = newFnOut;
						currentSoln = randNeighb;
						currentSatisfiesBound = satisfiesBound;

						// logger.log(DebugLevel.DEBUG,
						// "Neighb is worse, but accept. Current solns : "
						// + solns);

					} else {

						currentSoln.setShdReverseInd(true);
						// logger.log(DebugLevel.DEBUG,
						// "Neighb is worse, discard. Current solns : "
						// + solns);

					}
				}

				// Keep track of the best soln.
				if (Math.abs(newFnOut - bestFnOut) <= Config.FLOAT_EQUALIY_EPSILON
						&& satisfiesBound) {
					// TODO: Below is a hack. Maintain a heap of top-k solutions
					// in order to avoid too many solutions.
					// if (solns.size() < 20)
					solns.add(randNeighb);
				} else if (newFnOut < bestFnOut && satisfiesBound) {
					bestFnOut = newFnOut;
					solns.removeAll(solns);
					solns.add(randNeighb);
				}
			}

			temperature = temperature * alpha;
		}

		logger.log(ProdLevel.PROD, "\n\nTot worse solns : " + allWorse
				+ ", Accepted worse solns : " + accepted);

		return solns;
	}

	private void setFlexiEpsilonInd(Candidate soln, TargetDataset tgtDataset,
			MasterDataset mDataset, double maxPvt, double maxInd, long recSize) {
		if (soln == null)
			return;

		for (Objective bounded : boundedFns) {
			if (bounded instanceof CustomCleaningObjective
					|| bounded instanceof CleaningObjective) {
				double bOut = bounded.out(soln, tgtDataset, mDataset, maxPvt,
						maxInd, recSize);
				epsilonCleaning = bounded.getEpsilon() * bOut;
				// logger.log(ProdLevel.PROD,
				// "Ind Flexi Epsilon : " + bounded.getEpsilon());
			}
		}

	}

	public Objective getObjective() {
		return fn;
	}

	public Set<Objective> getBoundedObjectives() {
		return boundedFns;
	}

}
