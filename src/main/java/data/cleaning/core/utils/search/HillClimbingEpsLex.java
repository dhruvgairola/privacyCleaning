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
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.objectives.Objective;

/**
 * Lexicographic, where each objective has an associated threshold.
 * 
 * @author dhruvgairola
 *
 */
public class HillClimbingEpsLex extends Search {
	final private List<Objective> fns;
	final private InitStrategy strategy;
	final private IndNormStrategy indNormStrat;

	public HillClimbingEpsLex(List<Objective> fns, InitStrategy strategy,
			IndNormStrategy indNormStrat) {
		this.fns = fns;
		this.strategy = strategy;
		this.indNormStrat = indNormStrat;
	}

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

		long recSize = positionToChoices.keySet().size() * cols.size();

		if (recSize <= 0)
			return null;

		for (int i = 0; i < fns.size(); i++) {

			if (i == 0) {
				HillClimbingThreshold h = new HillClimbingThreshold(fns.get(i),
						strategy, indNormStrat);
				solns.addAll(h.calcOptimalSolns(constraint, tgtMatches,
						tgtDataset, mDataset, table, shdReturnInit));
			} else {
				solns = prune(constraint, solns, fns.get(i), tgtDataset,
						mDataset, table, maxPvt, maxInd, recSize);
			}

		}

		return solns;
	}

	private Set<Candidate> prune(Constraint constraint, Set<Candidate> toPrune,
			Objective objective, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table, double maxPvt,
			double maxInd, long recSize) {
		Set<Candidate> pruned = new HashSet<>();

		for (Candidate can : toPrune) {
			double out = objective.out(can, tgtDataset, mDataset, maxPvt,
					maxInd, recSize);

			if (out <= objective.getEpsilon()) {
				pruned.add(can);
			}
		}

		return pruned;
	}

}
