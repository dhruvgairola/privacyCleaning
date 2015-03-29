package data.cleaning.core.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import data.cleaning.core.service.dataset.impl.Record;

public class HashMapIndex {

	private static final Logger logger = Logger.getLogger(HashMapIndex.class);
	private Map<String, Map<String, Set<Integer>>> colToValToRIds;
	private Map<Integer, Record> rIdToRec;
	private Set<Integer> allRids;

	public HashMapIndex() {
		this.rIdToRec = new HashMap<>();
		this.colToValToRIds = new HashMap<>();
		this.allRids = new HashSet<>();
	}

	public void buildIndex(List<Record> records, List<String> cols) {
		logger.log(DebugLevel.DEBUG, "Building hashmap index.");
		for (int i = 0; i < cols.size(); i++) {
			String col = cols.get(i);
			Map<String, Set<Integer>> valToRIds = new HashMap<>();

			for (Record r : records) {
				String val = r.getColsToVal().get(col);

				if (!valToRIds.containsKey(val)) {
					Set<Integer> recIds = new HashSet<>();
					recIds.add((int) r.getId());
					valToRIds.put(val, recIds);
				} else {
					Set<Integer> recIds = valToRIds.get(val);
					recIds.add((int) r.getId());
				}

				allRids.add((int) r.getId());
				rIdToRec.put((int) r.getId(), r);
			}

			colToValToRIds.put(col, valToRIds);
		}

		logger.log(DebugLevel.DEBUG, "Built hashmap index.");
	}

	public Set<String> searchCol(Record r, List<String> andQueryCols,
			List<String> negationQueryCols, String colName) {
		Map<String, String> colsToVal = r.getColsToVal();
		Set<Integer> result = new HashSet<>(allRids);
		Set<String> resultStr = new HashSet<>();

		if (andQueryCols != null && !andQueryCols.isEmpty()) {
			for (int i = 0; i < andQueryCols.size(); i++) {
				String andQCol = andQueryCols.get(i);
				String val = colsToVal.get(andQCol);
				Map<String, Set<Integer>> valToRids = colToValToRIds
						.get(andQCol);
				Set<Integer> rIds = valToRids.get(val);
				result.retainAll(rIds);
			}
		}

		if (negationQueryCols != null && !negationQueryCols.isEmpty()) {
			for (int i = 0; i < negationQueryCols.size(); i++) {
				String negQCol = negationQueryCols.get(i);
				String val = colsToVal.get(negQCol);
				Map<String, Set<Integer>> valToRids = colToValToRIds
						.get(negQCol);
				Set<Integer> negRIds = new HashSet<>(allRids);
				negRIds.removeAll(valToRids.get(val));
				result.retainAll(negRIds);
			}
		}

		if (colName == null) {
			for (int ret : result) {
				Record rec = rIdToRec.get(ret);
				resultStr.add(rec.getColsToVal().values().iterator().next());
			}
		} else {
			for (int ret : result) {
				Record rec = rIdToRec.get(ret);
				resultStr.add(rec.getColsToVal().get(colName));
			}
		}

		return resultStr;
	}

	public Set<Integer> search(Record r, List<String> andQueryCols) {
		Map<String, String> colsToVal = r.getColsToVal();
		Set<Integer> result = new HashSet<>(allRids);

		for (int i = 0; i < andQueryCols.size(); i++) {
			String andQCol = andQueryCols.get(i);
			String val = colsToVal.get(andQCol);
			Map<String, Set<Integer>> valToRids = colToValToRIds.get(andQCol);
			Set<Integer> rIds = valToRids.get(val);
			result.retainAll(rIds);
		}

		return result;
	}

}
