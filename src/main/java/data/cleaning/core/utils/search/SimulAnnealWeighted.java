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
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

public class SimulAnnealWeighted extends Search {
	final double finalTemperature;
	final double initTemperature;
	final double bestEnergy;
	// Used in temp decay function.
	final double alpha;
	final private List<Objective> weightedFns;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;

	public SimulAnnealWeighted(List<Objective> weightedFns, double initTemp,
			double finalTemperature, double alpha, double bestEnergy,
			InitStrategy strategy, IndNormStrategy indNormStrat) {
		this.initTemperature = initTemp;
		this.finalTemperature = finalTemperature;
		this.alpha = alpha;
		this.weightedFns = weightedFns;
		this.bestEnergy = bestEnergy;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

	/*
	 * More than 1 solution can be found.
	 */
	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table) {
		int numIter = (int) Math.ceil(Math.log(finalTemperature
				/ initTemperature)
				/ Math.log(alpha));

		// logger.log(ProdLevel.PROD, "\n\nTheoretically max iterations : "
		// + numIter);

		// LoadingCache<List<Recommendation>, Integer> countCache = CacheBuilder
		// .newBuilder().maximumSize(100)
		// .expireAfterAccess(1, TimeUnit.SECONDS)
		// .build(new CacheLoader<List<Recommendation>, Integer>() {
		// public Integer load(List<Recommendation> recs) {
		// return backupCache.get(recs);
		// }
		// });

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

		Candidate initialSoln = currentSoln;

		double bestFnOut = Double.MAX_VALUE;
		double currentFnOut = 0d;

		for (Objective weightedFn : weightedFns) {
			currentFnOut += weightedFn.out(currentSoln, tgtDataset, mDataset,
					maxPvt, maxInd, recSize) * weightedFn.getWeight();
		}

		// logger.log(ProdLevel.PROD, "Out : " + currentFnOut);

		// How many worse solutions are accepted throughout this process?
		int accepted = 0;
		int allWorse = 0;
		int iter = 0;
		while (temperature > finalTemperature) {
			iter++;

			if (currentFnOut <= bestEnergy) {
				logger.log(ProdLevel.PROD, "Best energy (" + bestEnergy
						+ ") was reached.");
				return solns;
			}

			// List<Candidate> neighbs = new ArrayList<>(getNeighbs(currentSoln,
			// positionToChoices));
			// if (neighbs.isEmpty())
			// break;
			// Steps per temp in order to stabilize the solutions.
			for (int step = 0; step < Config.STEPS_PER_TEMP; step++) {
				// int randNum = rand.nextInt(neighbs.size());
				// Candidate randNeighb = neighbs.get(randNum);
				// logger.log(SecurityLevel.PROD,
				// "Tot neighb : " + neighbs.size());
				// logger.log(SecurityLevel.PROD, "Neighb num : " +
				// randNum);

				Candidate randNeighb = getRandNeighb(numBitFlipNeighb,
						numChoiceNeighb, currentSoln, positionToChoices);

				if (randNeighb == null)
					break;

				List<Recommendation> sRecs = randNeighb.getRecommendations();

				if (sRecs.isEmpty())
					continue;

				// logger.log(
				// DebugLevel.DEBUG,
				// "\nNeighb: "
				// + sRecs
				// + ", \nDiff wrt current soln (added): "
				// + randNeighb.getAdded()
				// + ", \nDiff wrt current soln (removed): "
				// + randNeighb.getRemoved()
				// + ", \nType: "
				// + randNeighb.getNeighbType().name()
				// + ", \nSign: "
				// + Arrays.toString(randNeighb.getSignatureCopy())
				// + ", \nTemperature : "
				// + Math.round(temperature * 100) / 100.0d
				// + ", \nIteration : " + iter);

				if (countCache.containsKey(sRecs)) {
					int countNeighb = countCache.get(sRecs);

					if (countNeighb + 1 > Config.SA_REPEAT_NEIGHB_THRESHOLD) {
						logger.log(ProdLevel.PROD, "Same neighb " + randNeighb
								+ " was seen "
								+ Config.SA_REPEAT_NEIGHB_THRESHOLD
								+ " times. Terminating.");
						// Just return something.
						if (solns.isEmpty()) {
							solns.add(initialSoln);
						}
						return solns;
					} else {
						countCache.put(sRecs, countNeighb + 1);
					}
				} else {
					countCache.put(sRecs, 1);
				}

				double fnout = 0d;
				// StringBuilder sb = new StringBuilder();
				for (Objective weightedFn : weightedFns) {
					double objOut = weightedFn.out(randNeighb, tgtDataset,
							mDataset, maxPvt, maxInd, recSize);
					fnout += objOut * weightedFn.getWeight();

					// TODO: Removed this debugging.
					// sb.append(weightedFn.getClass().getSimpleName()
					// + "(norm)[weight=" + weightedFn.getWeight()
					// + "] : " + objOut + " \n");
					//
					// if (weightedFn.getClass().getSimpleName()
					// .equals("PrivacyObjective")) {
					// sb.append(weightedFn.getClass().getSimpleName()
					// + " (unnorm) : " + (objOut * maxPvt) + " \n");
					// } else if (weightedFn.getClass().getSimpleName()
					// .equals("CleaningObjective")) {
					// sb.append("Upper bound on ind : " + maxInd + " \n");
					// sb.append(weightedFn.getClass().getSimpleName()
					// + " (unnorm) : " + (objOut * maxInd) + " \n");
					// } else {
					// sb.append(weightedFn.getClass().getSimpleName()
					// + " (unnorm) : " + (objOut * recSize) + " \n");
					// }

				}
				// sb.append("Iteration : " + iter);

				// randNeighb.setDebugging(sb.toString());

				// logger.log(DebugLevel.DEBUG, "Out : " + fnout);

				double newFnOut = fnout;

				double delta = currentFnOut - newFnOut;

				if (Math.abs(delta) <= Config.FLOAT_EQUALIY_EPSILON
						|| delta > 0) {
					currentFnOut = newFnOut;
					currentSoln = randNeighb;

					if (Math.abs(delta) <= Config.FLOAT_EQUALIY_EPSILON) {
						// logger.log(DebugLevel.DEBUG,
						// "Neighb is equally good.");
					} else {
						// logger.log(DebugLevel.DEBUG, "Neighb is better.");
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

						// logger.log(DebugLevel.DEBUG,
						// "Neighb is worse, but ACCEPT.");

					} else {
						currentSoln.setShdReverseInd(true);

						// logger.log(DebugLevel.DEBUG,
						// "Neighb is worse, DISCARD.");
					}
				}

				// Keep track of the best soln.
				if (Math.abs(newFnOut - bestFnOut) <= Config.FLOAT_EQUALIY_EPSILON) {
					solns.add(randNeighb);
				} else if (newFnOut < bestFnOut) {
					bestFnOut = newFnOut;
					solns.removeAll(solns);
					solns.add(randNeighb);
				}
			}

			temperature = temperature * alpha;
		}

		logger.log(ProdLevel.PROD, "\n\nTot worse solns : " + allWorse
				+ ", Accepted worse solns : " + accepted);

		if (solns.isEmpty()) {
			solns.add(initialSoln);
		}

		return solns;
	}

}
