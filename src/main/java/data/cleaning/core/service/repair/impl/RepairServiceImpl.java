package data.cleaning.core.service.repair.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.matching.MatchingService;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.RepairService;
import data.cleaning.core.utils.Pair;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.search.Search;

@Service("repairService")
public class RepairServiceImpl implements RepairService {
	@Autowired
	protected DatasetService datasetService;
	@Autowired
	protected MatchingService privacyService;

	private static final Logger logger = Logger
			.getLogger(RepairServiceImpl.class);

	@Override
	public Violations calcViolations(List<Record> records, Constraint constraint) {
		// TODO: Indexing the tgtDataset would improve performance.

		Violations vs = new Violations();

		List<String> ants = constraint.getAntecedentCols();
		List<String> cons = constraint.getConsequentCols();

		Multimap<String, Record> violMap = ArrayListMultimap.create();
		Map<String, Set<String>> antsToCons = new HashMap<String, Set<String>>();

		for (Record record : records) {
			String a = record.getRecordStr(ants);
			Set<String> c = null;

			if (antsToCons.containsKey(a)) {
				c = antsToCons.get(a);
				c.add(record.getRecordStr(cons));
			} else {
				c = new HashSet<>();
				c.add(record.getRecordStr(cons));
				antsToCons.put(a, c);
			}
		}

		for (Record record : records) {
			String a = record.getRecordStr(ants);

			if (antsToCons.containsKey(a)) {
				if (antsToCons.get(a).size() > 1) {
					violMap.put(a, record);
				}

			}
		}

		vs.setViolMap(violMap);
		vs.setConstraint(constraint);
		return vs;
	}

	@Override
	public Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, Search search, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table) {

		return search.calcOptimalSolns(constraint, tgtMatches, tgtDataset,
				mDataset, table);
	}

	@Override
	public List<Set<Long>> subsetViolsBySize(Violations viols) {
		List<Set<Long>> subset = new ArrayList<>();
		Multimap<String, Record> violMap = viols.getViolMap();

		for (String key : violMap.keySet()) {
			Collection<Record> recs = violMap.get(key);

			Set<Long> v = new HashSet<>();
			for (Record r : recs) {
				v.add(r.getId());
			}

			subset.add(v);
		}

		Collections.sort(subset, new Comparator<Set<Long>>() {

			@Override
			public int compare(Set<Long> o1, Set<Long> o2) {
				if (o1.size() > o2.size()) {
					return -1;
				} else if (o1.size() < o2.size()) {
					return 1;
				} else {
					return 0;
				}
			}
		});

		return subset;
	}

	@Override
	public List<Violations> orderViolations(TargetDataset tgtDataset) {
		List<Constraint> constraints = tgtDataset.getConstraints();
		List<Violations> orderedViolations = new ArrayList<>();
		List<Record> tgtRecs = tgtDataset.getRecords();

		for (Constraint constraint : constraints) {
			Violations v = calcViolations(tgtRecs, constraint);
			orderedViolations.add(v);
		}

		Collections.sort(orderedViolations, new Comparator<Violations>() {
			@Override
			public int compare(Violations v1, Violations v2) {
				int sizeV1 = v1.getViolMap().size();
				int sizeV2 = v2.getViolMap().size();
				return -1 * Integer.compare(sizeV1, sizeV2);
			}
		});

		return orderedViolations;
	}

}
