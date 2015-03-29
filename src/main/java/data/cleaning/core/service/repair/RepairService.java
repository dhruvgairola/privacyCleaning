package data.cleaning.core.service.repair;

import java.util.List;
import java.util.Set;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.search.Search;

public interface RepairService {
	Violations calcViolations(List<Record> records, Constraint constraint);

	Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, Search search, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table, boolean shdReturnInit);

	/**
	 * Bigger violation chunk is ordered first.
	 * 
	 * @param viols
	 * @return
	 */
	List<Set<Long>> subsetViolsBySize(Violations viols);

	List<Violations> orderViolations(TargetDataset tgtDataset);

}
