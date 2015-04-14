package data.cleaning.core.service.errgen.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.io.ByteStreams;

import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.Dataset;
import data.cleaning.core.service.dataset.impl.DatasetType;
import data.cleaning.core.service.dataset.impl.GroundTruthDataset;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.errgen.ErrgenService;
import data.cleaning.core.service.repair.RepairService;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.DebugLevel;
import data.cleaning.core.utils.DistanceMeasures;
import data.cleaning.core.utils.LuceneIndex;
import data.cleaning.core.utils.Pair;
import data.cleaning.core.utils.ProdLevel;

@Service("errgenService")
public class ErrgenServiceImpl implements ErrgenService {
	@Autowired
	@Qualifier(value = "datasetService")
	protected DatasetService datasetService;
	@Autowired
	@Qualifier(value = "repairService")
	protected RepairService repairService;
	private Random rand;
	private long dummyErrorRecordId;

	public ErrgenServiceImpl() {
		this.rand = new Random(Config.SEED);
	}

	private static final Logger logger = Logger
			.getLogger(ErrgenServiceImpl.class);

	public Dataset loadDataset(String url, String name, String fdUrl,
			DatasetType dType, char separator, char quoteChar) {
		InputStream is = null;
		InputStream is2 = null;
		try {
			is = new FileInputStream(url);
			is2 = new FileInputStream(fdUrl);
			List<Record> records = constructRecords(
					ByteStreams.toByteArray(is), separator, quoteChar);

			Dataset dataset = null;
			if (dType == DatasetType.TARGET) {
				dataset = new TargetDataset(records);
			} else if (dType == DatasetType.MASTER) {
				dataset = new MasterDataset(records);
			} else if (dType == DatasetType.GROUND_TRUTH) {
				dataset = new GroundTruthDataset(records);
			} else {
				dataset = new TargetDataset(records);
			}

			dataset.setId(1);
			dataset.setUrl(url);
			dataset.setName(name);
			dataset.setConstraints(constructConstraints(ByteStreams
					.toByteArray(is2)));
			return dataset;
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} finally {
			try {
				is.close();
				is2.close();
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}
		}

		return null;

	}

	private List<Constraint> constructConstraints(byte[] constraints) {
		List<Constraint> cs = new ArrayList<>();
		CSVReader reader = null;

		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(constraints)));

			reader = new CSVReader(in, ',');

			String[] nextLine;

			long conId = 0;
			while ((nextLine = reader.readNext()) != null) {
				StringBuilder antecedent = new StringBuilder();
				for (int i = 0; i < nextLine.length - 1; i++) {
					antecedent.append(nextLine[i].trim() + ",");
				}

				Constraint constraint = new Constraint();
				constraint.setId(conId);
				constraint.setDatasetid(1);
				constraint.setAntecedent(antecedent.substring(0,
						antecedent.length() - 1));
				constraint.setConsequent(nextLine[nextLine.length - 1]);
				conId++;
				cs.add(constraint);

			}
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}
		}

		return cs;
	}

	private List<Record> constructRecords(byte[] bytes, char separator,
			char quoteChar) {
		List<Record> records = new ArrayList<>();
		CSVReader reader = null;

		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(bytes)));

			if (separator == (char) -1 && quoteChar == (char) -1) {
				reader = new CSVReader(in);
			} else if (quoteChar == (char) -1) {
				reader = new CSVReader(in, separator);
			} else {
				reader = new CSVReader(in, separator, quoteChar);
			}

			String[] nextLine;
			int count = 0;
			Map<Integer, String> cols = new HashMap<>();

			while ((nextLine = reader.readNext()) != null) {
				if (count == 0) {
					for (int i = 0; i < nextLine.length; i++) {
						cols.put(i, nextLine[i]);
					}
				} else {
					Record r = new Record(count);
					Map<String, String> colsToVal = new LinkedHashMap<>();

					for (int i = 0; i < nextLine.length; i++) {
						if (nextLine[i] == null) {
							colsToVal.put(cols.get(i).trim(), "");
						} else {
							String val = nextLine[i].trim();
							// val.intern();
							// Fix for annoying bug with csvreader.
							if ("\"".equals(val) || "'".equals(val)) {
								colsToVal.put(cols.get(i).trim(), "");
							} else {
								colsToVal.put(cols.get(i).trim(), val);
							}
						}

					}

					r.setColsToVal(colsToVal);
					records.add(r);
				}

				count++;
			}
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}
		}

		return records;
	}

	/**
	 * Returns converse result of doesntSatisfyFDQuery. Complicated to
	 * understand.
	 * 
	 * @param colsToVal
	 * @param remainderAnt
	 *            - antecedents minus the current cell attribute being
	 *            considered.
	 * @param consq
	 * @return
	 */
	@SuppressWarnings("unused")
	private Query doesSatisfyFDQuery(Map<String, String> colsToVal,
			List<String> remainderAnt, List<String> consq) {
		BooleanQuery bQuery = new BooleanQuery();

		// This check is for FDs with only 1 antecedent bec remainderAnt will be
		// empty if current cell attribute is an antecedent.
		if (remainderAnt == null || remainderAnt.isEmpty()) {
			for (String consqCol : consq) {
				PhraseQuery query = new PhraseQuery();
				query.add(new Term(consqCol, colsToVal.get(consqCol)));
				bQuery.add(query, BooleanClause.Occur.MUST);
			}

		} else {
			BooleanQuery categoryQuery = new BooleanQuery();
			for (String raCol : remainderAnt) {

				PhraseQuery query1 = new PhraseQuery();

				query1.add(new Term(raCol, colsToVal.get(raCol)));
				categoryQuery.add(new BooleanClause(query1,
						BooleanClause.Occur.MUST));
			}

			for (String consqCol : consq) {

				PhraseQuery query2 = new PhraseQuery();

				query2.add(new Term(consqCol, colsToVal.get(consqCol)));
				categoryQuery.add(new BooleanClause(query2,
						BooleanClause.Occur.MUST));
			}

			// Same remainderAnt and same consq as the current record do satisfy
			// FD.
			bQuery.add(new BooleanClause(categoryQuery,
					BooleanClause.Occur.SHOULD));

			// OR, different remainderAnt does satisfy FD.
			for (String raCol : remainderAnt) {
				BooleanQuery categoryQuery2 = new BooleanQuery();

				// Hack for : (lucene) The negation operator cannot be used with
				// just one term.
				PhraseQuery qDum = new PhraseQuery();
				qDum.add(new Term("dummy", "foo"));
				categoryQuery2.add(new BooleanClause(qDum,
						BooleanClause.Occur.MUST));

				// Negation
				PhraseQuery q = new PhraseQuery();
				q.add(new Term(raCol, colsToVal.get(raCol)));
				categoryQuery2.add(new BooleanClause(q,
						BooleanClause.Occur.MUST_NOT));

				bQuery.add(new BooleanClause(categoryQuery2,
						BooleanClause.Occur.SHOULD));

			}

		}

		logger.log(DebugLevel.DEBUG, "Exact lucene query : " + bQuery);

		return bQuery;

	}

	public Query doesntSatisfyFDQuery(Map<String, String> colsToVal,
			List<String> andQueryCols, List<String> negationQueryCols) {
		BooleanQuery bQuery = new BooleanQuery();

		if (andQueryCols != null && !andQueryCols.isEmpty()) {

			for (String andQueryCol : andQueryCols) {
				PhraseQuery query = new PhraseQuery();
				query.add(new Term(andQueryCol, colsToVal.get(andQueryCol)));
				// BooleanClause.Occur.MUST == And
				bQuery.add(query, BooleanClause.Occur.MUST);
			}

		} else {
			// Hack for : (lucene) The negation operator cannot be used with
			// just one term. We had previously indexed a dummy column. We can
			// use this now if andQueryCols is null or empty.
			PhraseQuery query = new PhraseQuery();
			query.add(new Term("dummy", "foo"));
			bQuery.add(query, BooleanClause.Occur.MUST);
		}

		if (negationQueryCols != null && !negationQueryCols.isEmpty()) {

			for (String negationQueryCol : negationQueryCols) {

				PhraseQuery query = new PhraseQuery();
				query.add(new Term(negationQueryCol, colsToVal
						.get(negationQueryCol)));
				// BooleanClause.Occur.MUST_NOT == Negation
				bQuery.add(query, BooleanClause.Occur.MUST_NOT);

			}
		}

		logger.log(DebugLevel.DEBUG, "Exact lucene query : " + bQuery);

		return bQuery;

	}

	public boolean isSatisfied(Constraint constraint, List<Record> recs) {
		// Slow
		List<String> ants = constraint.getAntecedentCols();
		List<String> cons = constraint.getConsequentCols();

		Map<String, Set<String>> antsToCons = new HashMap<String, Set<String>>();

		for (Record r : recs) {
			String a = r.getRecordStr(ants);
			Set<String> c = null;

			if (antsToCons.containsKey(a)) {
				c = antsToCons.get(a);
				c.add(r.getRecordStr(cons));

				if (c.size() > 1)
					return false;
			} else {
				c = new HashSet<>();
				c.add(r.getRecordStr(cons));
				antsToCons.put(a, c);
			}

		}

		return true;
	}

	@SuppressWarnings("deprecation")
	@Override
	public TargetDataset addErrorsRandom(List<ErrorType> types,
			int desiredTgtSize, int roughChunkSize, double percentageAnt,
			double percentageCons, String gtUrl, String tgtOutUrl,
			String tgtOutName, String fdUrl, String errMetadataUrl,
			char separator, char quoteChar) throws Exception {
		GroundTruthDataset gtDataset = datasetService.loadGroundTruthDataset(
				gtUrl, "", fdUrl, 1, separator, quoteChar);
		List<Constraint> constraints = gtDataset.getConstraints();

		List<Record> groundTruth = gtDataset.getRecords();

		if (groundTruth.size() > desiredTgtSize)
			throw new Exception(
					"Ground truth cannot be larger than target size.");

		List<Record> tgtRecs = new ArrayList<>();

		int numErrsCons = (int) ((double) desiredTgtSize * percentageCons);
		int numErrsAnt = (int) ((double) desiredTgtSize * percentageAnt);
		int numClean = desiredTgtSize - numErrsAnt - numErrsCons;

		int seedSetSize = (int) ((numErrsCons + numErrsAnt) / roughChunkSize);
		int cleanSetSize = groundTruth.size() - seedSetSize;

		// No record will have this id or above.
		dummyErrorRecordId = numClean + groundTruth.size() + 1;
		// As we add recs to master, we keep track of the new gtIds and what
		// they originally referred to in gtDataset.
		Map<Long, Long> gtNewToOld = new HashMap<>();

		// Add clean recs first.
		for (int i = 0; i < numClean; i++) {
			Record r = groundTruth.get(i % cleanSetSize);
			// Add copies to avoid bugs.
			Record copyR = copyRecord(r);
			gtNewToOld.put(copyR.getId(), r.getId());
			tgtRecs.add(copyR);
		}

		// The clean ids are not part of the seedset from which errors are
		// created.
		List<Record> ss = groundTruth.subList(Math.min(numClean, cleanSetSize),
				groundTruth.size());
		List<Record> seedSet = new ArrayList<>();

		// Add copies to avoid bugs.
		for (Record r : ss) {
			Record copyR = copyRecord(r);
			gtNewToOld.put(copyR.getId(), r.getId());
			seedSet.add(copyR);
		}

		// Index the copies of the gt records.
		List<Record> toIdx = new ArrayList<>(tgtRecs);
		toIdx.addAll(new ArrayList<>(seedSet));

		Set<String> idxCols = new HashSet<>();
		for (Constraint constraint : constraints) {
			idxCols.addAll(constraint.getColsInConstraint());
		}

		// Assert.assertTrue(sanityCheckUniqueRids(toIdx));

		LuceneIndex idx = new LuceneIndex();
		idx.buildIndex(toIdx, new ArrayList<>(idxCols), Field.Index.ANALYZED);

		genErrors(constraints, seedSet, numErrsCons, types, tgtRecs, idx,
				gtNewToOld, true);

		genErrors(constraints, seedSet, numErrsAnt, types, tgtRecs, idx,
				gtNewToOld, false);

		// Assert.assertTrue(sanityCheckUniqueRids(tgtRecs));

		// Need to do this in order to get the correct ids for the target
		// tuples.
		tgtRecs = reassignRecordIds(tgtRecs);

		// Due to attribute overlaps within FDs, and the way that antecedent and
		// consq errors were generated, we might have generated a bit too many
		// or a bit too few errors. Hence, we either trim or add to "tgtRecs" in
		// order to ensure that at least the consq errors fall within the range
		// [percentageCons, percentageCons + percentageAnt] of the "tgtRecs"
		// size. At this point, we don't really care about the "types" of
		// errors.
		trimOrAdd(constraints, tgtRecs, numErrsCons, numErrsAnt, desiredTgtSize);

		// Need to do this in order to get the correct ids for the target
		// tuples.
		tgtRecs = reassignRecordIds(tgtRecs);

		// Assert.assertTrue(sanityCheckUniqueRids(tgtRecs));

		List<ErrorMetadata> emds = new ArrayList<>();
		for (Record r : tgtRecs) {
			if (r.getErrMetadata() != null)
				emds.add(r.getErrMetadata());
		}

		// Adding and removing recs can mess up the desiredTgtSize.
		balanceTgtSize(constraints, tgtRecs, desiredTgtSize, emds);

		// Assert.assertTrue(sanityCheckUniqueRids(tgtRecs));

		// Need to do this in order to get the correct ids for the target
		// tuples.
		tgtRecs = reassignRecordIds(tgtRecs);

		saveErrorMetadata(constraints, tgtRecs, errMetadataUrl, separator,
				quoteChar);

		datasetService.saveDataset(tgtRecs, tgtOutUrl, separator, quoteChar);

		return datasetService.loadTargetDataset(tgtOutUrl, tgtOutName, fdUrl,
				separator, quoteChar);
	}

	private boolean sanityCheckUniqueRids(List<Record> tgtRecs) {
		Set<Long> rids = new HashSet<>();

		for (Record t : tgtRecs) {
			if (rids.contains(t.getId())) {
				return false;
			}
			rids.add(t.getId());
		}
		return true;

	}

	private void saveErrorMetadata(List<Constraint> constraints,
			List<Record> tgtRecs, String errMetadataUrl, char separator,
			char quoteChar) {
		CSVWriter writer;
		try {
			if (quoteChar == (char) -1) {
				writer = new CSVWriter(new FileWriter(errMetadataUrl),
						separator, CSVWriter.NO_QUOTE_CHARACTER);
			} else {
				writer = new CSVWriter(new FileWriter(errMetadataUrl),
						separator, quoteChar);
			}

			String[] colsArr = new String[] { "tId", "Col", "OrigVal",
					"ErrVal", "gtId" };

			writer.writeNext(colsArr);

			for (int i = 0; i < tgtRecs.size(); i++) {
				Record record = tgtRecs.get(i);
				ErrorMetadata emd = record.getErrMetadata();
				if (emd == null)
					continue;
				Map<String, String> errColsToVal = emd.getErrorsColsToVal();
				Map<String, String> origColsToVal = emd.getOrigColsToVal();

				if (errColsToVal != null && !errColsToVal.isEmpty()) {
					List<String> vals = new ArrayList<>();

					for (Map.Entry<String, String> e : errColsToVal.entrySet()) {
						// Assert.assertTrue(record.getId() == emd.getTid());

						vals.add(emd.getTid() + "");
						vals.add(e.getKey());
						vals.add(origColsToVal.get(e.getKey()).trim());
						vals.add(e.getValue().trim());
						vals.add(emd.getGtId() + "");
					}
					writer.writeNext(vals.toArray(new String[vals.size()]));
				}

			}

			writer.close();
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

	}

	private void saveErrorMetadata(List<ErrorMetadata> emds,
			String errMetadataUrl, char separator, char quoteChar) {
		CSVWriter writer;
		try {
			if (quoteChar == (char) -1) {
				writer = new CSVWriter(new FileWriter(errMetadataUrl),
						separator, CSVWriter.NO_QUOTE_CHARACTER);
			} else {
				writer = new CSVWriter(new FileWriter(errMetadataUrl),
						separator, quoteChar);
			}

			String[] colsArr = new String[] { "tId", "Col", "OrigVal",
					"ErrVal", "gtId" };

			writer.writeNext(colsArr);

			for (ErrorMetadata emd : emds) {
				if (emd == null)
					continue;
				Map<String, String> errColsToVal = emd.getErrorsColsToVal();
				Map<String, String> origColsToVal = emd.getOrigColsToVal();

				if (errColsToVal != null && !errColsToVal.isEmpty()) {
					List<String> vals = new ArrayList<>();

					for (Map.Entry<String, String> e : errColsToVal.entrySet()) {
						// Assert.assertTrue(record.getId() == emd.getTid());

						vals.add(emd.getTid() + "");
						vals.add(e.getKey());
						vals.add(origColsToVal.get(e.getKey()).trim());
						vals.add(e.getValue().trim());
						vals.add(emd.getGtId() + "");
					}
					writer.writeNext(vals.toArray(new String[vals.size()]));
				}

			}

			writer.close();
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

	}

	private List<Record> reassignRecordIds(List<Record> tgtRecs) {
		List<Record> reassigned = new ArrayList<>();

		for (long i = 0; i < tgtRecs.size(); i++) {
			Record tgtRec = tgtRecs.get((int) i);

			// Record id is final, so do it this way.
			Record r = new Record(i + 1);
			r.setColsToVal(new LinkedHashMap<>(tgtRec.getColsToVal()));
			r.setErrMetadata(copyErrorMetadata(tgtRec.getErrMetadata()));

			if (r.getErrMetadata() != null) {
				r.getErrMetadata().setTid(i + 1);
			}

			reassigned.add(r);
		}

		return reassigned;
	}

	private ErrorMetadata copyErrorMetadata(ErrorMetadata emd) {
		if (emd != null) {
			ErrorMetadata newEmd = new ErrorMetadata();

			newEmd.setTid(emd.getTid());
			newEmd.setGtId(emd.getGtId());
			newEmd.setErrorsColsToVal(new LinkedHashMap<>(emd
					.getErrorsColsToVal()));
			newEmd.setOrigColsToVal(new LinkedHashMap<>(emd.getOrigColsToVal()));
			return newEmd;
		}

		return null;
	}

	public Record copyRecord(Record r) {
		Record newR = new Record(dummyErrorRecordId);
		newR.setColsToVal(new LinkedHashMap<>(r.getColsToVal()));
		ErrorMetadata emd = r.getErrMetadata();
		newR.setErrMetadata(copyErrorMetadata(emd));
		dummyErrorRecordId++;
		return newR;
	}

	private void trimOrAdd(List<Constraint> constraints, List<Record> tgtRecs,
			int numErrsCons, int numErrsAnt, int desiredTgtSize) {
		// How many FDs does 1 record violate?
		Multiset<Record> countsFDsViol = HashMultiset.create();

		int totViols = 0;
		for (int i = 0; i < constraints.size(); i++) {
			Constraint constraint = constraints.get(i);
			Violations v = repairService.calcViolations(tgtRecs, constraint);
			Multimap<String, Record> vMap = v.getViolMap();

			for (String pattern : vMap.keySet()) {
				Collection<Record> recs = vMap.get(pattern);
				for (Record r : recs) {
					countsFDsViol.add(r);
				}
			}

			totViols += vMap.size();
		}

		logger.log(ProdLevel.PROD, "Total viols before :" + totViols);

		if (totViols < numErrsCons) {
			// Maintain min heap.
			Queue<Violations> viols = new PriorityQueue<>(constraints.size(),
					new Comparator<Violations>() {

						@Override
						public int compare(Violations o1, Violations o2) {
							if (o1.getViolMap().size() > o2.getViolMap().size()) {
								return 1;
							} else if (o1.getViolMap().size() < o2.getViolMap()
									.size()) {
								return -1;
							} else {
								return 0;
							}
						}
					});

			for (Constraint constraint : constraints) {
				Violations v = repairService
						.calcViolations(tgtRecs, constraint);
				viols.add(v);
			}

			int numAdded = 0;

			int deficit = numErrsCons - totViols;
			logger.log(ProdLevel.PROD, deficit
					+ " more errors need to be added.");

			while (numAdded <= deficit) {
				// Add.
				Violations v = viols.peek();
				Multimap<String, Record> vMap = v.getViolMap();
				boolean shdBreak = false;

				for (String ant : vMap.keySet()) {
					Collection<Record> vRecs = vMap.get(ant);
					for (Record vRec : vRecs) {
						ErrorMetadata vemd = vRec.getErrMetadata();

						// Add viols which were not injected.
						if (vemd == null || vemd.getErrorsColsToVal() == null
								|| vemd.getErrorsColsToVal().isEmpty()) {
							Record copy = copyRecord(vRec);
							tgtRecs.add(copy);
							numAdded += countsFDsViol.count(vRec);
							shdBreak = true;
							break;
						}
					}

					if (shdBreak)
						break;
				}

			}

		} else if (totViols >= numErrsCons) {
			// Maintain max heap.
			Queue<Violations> viols = new PriorityQueue<>(constraints.size(),
					new Comparator<Violations>() {

						@Override
						public int compare(Violations o1, Violations o2) {
							if (o1.getViolMap().size() > o2.getViolMap().size()) {
								return -1;
							} else if (o1.getViolMap().size() < o2.getViolMap()
									.size()) {
								return 1;
							} else {
								return 0;
							}
						}
					});

			for (Constraint constraint : constraints) {
				Violations v = repairService
						.calcViolations(tgtRecs, constraint);
				viols.add(v);
			}

			int numRemoved = 0;
			int surplus = totViols - numErrsCons;
			logger.log(ProdLevel.PROD, surplus + " errors need to be removed.");

			while (numRemoved <= surplus) {
				Violations v = viols.peek();
				Multimap<String, Record> vMap = v.getViolMap();

				if (vMap.isEmpty()) {
					logger.log(ProdLevel.PROD, "Cannot remove surplus.");
					break;
				}

				String pattern = vMap.keySet().iterator().next();
				Collection<Record> unsat = vMap.get(pattern);
				Record toRemove = unsat.iterator().next();
				// Below removes it from vMap also.
				unsat.remove(toRemove);
				tgtRecs.remove(toRemove);

				// Special case. Can't have a violation with only 1 record.
				if (unsat.size() == 1) {
					Record toAlsoRemove = unsat.iterator().next();
					unsat.remove(toAlsoRemove);
					tgtRecs.remove(toAlsoRemove);

					int count = countsFDsViol.count(toRemove)
							+ countsFDsViol.count(toAlsoRemove);
					numRemoved += count;
				} else {
					int count = countsFDsViol.count(toRemove);
					numRemoved += count;
				}

				if (vMap.isEmpty()) {
					viols.remove(v);
				}

				// logger.log(ProdLevel.PROD, "# " + numRemoved + ", Removed "
				// + toRemove);

			}
		} else {
			logger.log(ProdLevel.PROD, "Errors were generated perfectly!");
		}
	}

	/**
	 * Add or remove completely innocuous (don't violate any constraint) records
	 * to balance the tgt size.
	 * 
	 * @param tgtRecs
	 * @param desiredTgtSize
	 */
	private void balanceTgtSize(List<Constraint> constraints,
			List<Record> tgtRecs, int desiredTgtSize, List<ErrorMetadata> emds) {
		if (tgtRecs.size() == desiredTgtSize)
			return;

		List<Record> innocuous = findInnocuousRecs(constraints, tgtRecs, emds);

		// Very unlikely. Easier to print a msg instead of adding a complicated
		// fix.
		if (innocuous == null || innocuous.isEmpty()) {
			logger.log(ProdLevel.PROD,
					"Every single record in the created dataset violates some FD. "
							+ "Hence, we cannot hit the desired target size.");
			return;
		}

		if (tgtRecs.size() < desiredTgtSize) {
			// Pad the target dataset.
			logger.log(ProdLevel.PROD, "Padding target size. Deficit : "
					+ (desiredTgtSize - tgtRecs.size()));

			while (tgtRecs.size() < desiredTgtSize) {
				Record r = innocuous.get(rand.nextInt(innocuous.size()));
				Record newR = copyRecord(r);
				tgtRecs.add(newR);
			}

		} else {
			int surplus = tgtRecs.size() - desiredTgtSize;
			logger.log(ProdLevel.PROD, "Trimming target size. Surplus : "
					+ surplus);

			// Cannot hit target size even after removing all innocuous records.
			if (innocuous.size() < surplus) {
				tgtRecs.removeAll(innocuous);
			} else {
				tgtRecs.removeAll(innocuous.subList(0, surplus));
			}
		}

	}

	private List<Record> findInnocuousRecs(List<Constraint> constraints,
			List<Record> tgtRecs, List<ErrorMetadata> emds) {
		List<Record> innocuous = new ArrayList<>();
		Set<Record> viols = new HashSet<>();
		Set<Long> emdIds = getEmdIds(emds);

		for (Constraint constraint : constraints) {
			Violations v = repairService.calcViolations(tgtRecs, constraint);
			Multimap<String, Record> vMap = v.getViolMap();
			Set<Record> rs = new HashSet<>(vMap.values());
			viols.addAll(rs);
		}

		for (Record r : tgtRecs) {
			if (!viols.contains(r) && !emdIds.contains(r.getId())) {
				innocuous.add(r);
			}
		}

		return innocuous;
	}

	private void genErrors(List<Constraint> constraints, List<Record> seedSet,
			int numErrs, List<ErrorType> types, List<Record> tgtRecs,
			LuceneIndex idx, Map<Long, Long> gtNewToOld, boolean isConsError)
			throws Exception {
		float sum = 0;
		for (int i = 0; i < types.size(); i++) {
			ErrorType type = types.get(i);
			sum += type.getPercentage();
		}
		if (Math.abs(sum - 1.0f) > Config.FLOAT_EQUALIY_EPSILON)
			throw new Exception("The error types don't all sum up to 1.0.");

		int errsPerConstraint = numErrs / constraints.size();
		for (int i = 0; i < constraints.size(); i++) {
			Constraint constraint = constraints.get(i);

			// Special case to handle remainders.
			if (i == constraints.size() - 1) {
				errsPerConstraint = numErrs;
			}

			genErrors(constraint, seedSet, errsPerConstraint, types, tgtRecs,
					idx, gtNewToOld, isConsError);

			numErrs = numErrs - errsPerConstraint;
		}
	}

	/**
	 * Annoyingly confusing and complicated method.
	 * 
	 * @param constraint
	 * @param seedSet
	 * @param errsPerConstraint
	 * @param types
	 * @param tgtRecs
	 * @param idx
	 * @param gtNewToOld
	 * @param isConsError
	 * @throws Exception
	 */
	private void genErrors(Constraint constraint, List<Record> seedSet,
			int errsPerConstraint, List<ErrorType> types, List<Record> tgtRecs,
			LuceneIndex idx, Map<Long, Long> gtNewToOld, boolean isConsError)
			throws Exception {
		// Possible because generated errors wrt previous constraint could've
		// generated errs wrt current one.
		int initialErrs = repairService.calcViolations(tgtRecs, constraint)
				.getViolMap().size();
		if (initialErrs >= errsPerConstraint) {
			// This case can be handled by postprocessing (method trimOrAdd).
			return;
		} else {
			errsPerConstraint -= initialErrs;
		}

		// logger.log(ProdLevel.PROD, "Errors reqd : " + errsPerConstraint);
		List<String> ants = constraint.getAntecedentCols();
		List<String> cons = constraint.getConsequentCols();
		List<String> colsInConstraint = constraint.getColsInConstraint();

		// Remove the duplicates in the ground truth wrt constraint.
		Multimap<String, Record> uniquePInSeedSet = getUniquePatterns(ants,
				seedSet);
		List<String> uniqPatterns = new ArrayList<>(uniquePInSeedSet.keySet());
		int uniqPatternSize = uniqPatterns.size();
		Set<Long> seeds = new HashSet<>();
		List<Record> errors = new ArrayList<>();
		Record defaultError = null;
		long defaultGtId = 0;

		for (int i = 0; i < types.size(); i++) {
			ErrorType type = types.get(i);
			Map<Float, Float> simToDistribution = type.getSimToDistribution();

			int errsOfThisType = 0;

			// Special case to handle doubles in type.getPercentage() call.
			if (i == types.size() - 1) {
				errsOfThisType = errsPerConstraint;
			} else {
				errsOfThisType = (int) (type.getPercentage() * (double) errsPerConstraint);
				errsPerConstraint = errsPerConstraint - errsOfThisType;
			}

			double totDistr = 0d;
			NavigableMap<Integer, Float> numErrToSim = new TreeMap<>();
			if (simToDistribution != null && !simToDistribution.isEmpty()) {
				for (Map.Entry<Float, Float> en : simToDistribution.entrySet()) {
					float sim = en.getKey();
					float distribution = en.getValue();
					numErrToSim
							.put((int) ((float) errsOfThisType * (distribution + totDistr)),
									sim);
					totDistr += distribution;
				}
			}

			if (errsOfThisType == 1) {
				// Use the default error if only 1 new error needs to be added.
				if (errors != null && !errors.isEmpty()) {
					errors.add(genError(ants, cons, colsInConstraint, 0.9f,
							defaultError, type, defaultGtId, idx, isConsError));

				} else {
					throw new Exception(
							"It is impossible to have only 1 total error in a dataset. Minimum must be 2.");
				}
			} else {
				int errsAdded = 0;

				while (errsAdded < errsOfThisType) {
					float sim = 0.9f;

					if (!numErrToSim.isEmpty()) {
						Map.Entry<Integer, Float> nToS = numErrToSim
								.ceilingEntry(errsAdded);

						sim = nToS.getValue();
					}

					int selRec = rand.nextInt(uniqPatternSize);
					// Seed also becomes an error wrt to the FD.
					Record seed = uniquePInSeedSet
							.get(uniqPatterns.get(selRec)).iterator().next();

					// If seed is new.
					if (!seeds.contains(seed.getId())) {

						// If adding seed exceeds num allowed errors, then
						// don't add it to errors. Use default error..
						if (errsAdded + 1 == errsOfThisType) {

							errors.add(genError(ants, cons, colsInConstraint,
									sim, defaultError, type, defaultGtId, idx,
									isConsError));
							errsAdded++;
							break;
						} else {
							// Add seed.
							Record newS = copyRecord(seed);
							// Get the actual gtId of the seed (remember that we
							// had created a copy of the gt records hence we
							// need to do this).
							long actualGtId = gtNewToOld.get(seed.getId());

							if (newS.getErrMetadata() != null) {
								newS.getErrMetadata().setGtId(actualGtId);
							}

							// Let this be the default error.
							defaultError = newS;
							defaultGtId = actualGtId;

							errors.add(newS);
							seeds.add(seed.getId());

							errors.add(genError(ants, cons, colsInConstraint,
									sim, newS, type, actualGtId, idx,
									isConsError));
							errsAdded = errsAdded + 2;
						}
					} else {
						// Use existing seed gt copy.
						long actualGtId = gtNewToOld.get(seed.getId());
						errors.add(genError(ants, cons, colsInConstraint, sim,
								seed, type, actualGtId, idx, isConsError));
						errsAdded++;
					}
				}
			}

		}
		// logger.log(ProdLevel.PROD, "Errors added : " + errors.size());
		tgtRecs.addAll(errors);
	}

	private Multimap<String, Record> getUniquePatterns(List<String> cols,
			List<Record> recs) {
		Multimap<String, Record> pToRecs = ArrayListMultimap.create();

		for (Record r : recs) {
			String pattern = r.getRecordStr(cols);
			pToRecs.put(pattern, r);
		}

		return pToRecs;
	}

	/**
	 * A generated error is a completely new record with a new record id which
	 * exists outside of the ground truth.
	 * 
	 * @param ants
	 * @param cols
	 * @param record
	 * @param type
	 * @param newErrId
	 * @return
	 */
	private Record genError(List<String> ants, List<String> cons,
			List<String> colsInConstraint, double sim, Record record,
			ErrorType type, long gtId, LuceneIndex idx, boolean isConsError) {
		Record error = copyRecord(record);

		error.setColsToVal(new LinkedHashMap<>(record.getColsToVal()));

		int randColIdx = 0;
		List<String> randColList = null;
		String randColVal = "";

		if (isConsError) {
			randColIdx = rand.nextInt(cons.size());
			randColList = cons.subList(randColIdx, randColIdx + 1);
		} else {
			randColIdx = rand.nextInt(ants.size());
			randColList = ants.subList(randColIdx, randColIdx + 1);
		}
		randColVal = record.getRecordStr(randColList);
		String randCol = randColList.get(0);

		ErrorMetadata emd = new ErrorMetadata();
		emd.setGtId(gtId);
		// Keep track of original value.
		emd.addOrigValForCol(randCol, randColVal);
		error.setErrMetadata(emd);

		if (type.getType() == ErrorType.Type.IN_DOMAIN_SIMILAR) {
			Map<String, String> colToCols = idx.searchColAndCols(
					simConsqQuery(randCol, randColVal, sim), randCol,
					colsInConstraint, 5);
			String recStr = error.getRecordStr(colsInConstraint);
			List<String> simConsVals = new ArrayList<>();

			for (Map.Entry<String, String> en : colToCols.entrySet()) {
				String col = en.getKey();
				String cols = en.getValue();
				int levDist = DistanceMeasures.getLevDistance(recStr, cols);
				double sim2 = calcSim(levDist, recStr.length(), cols.length());

				if (sim2 >= sim) {
					simConsVals.add(col);
				}
			}

			if (simConsVals == null || simConsVals.isEmpty()) {
				error.modifyValForExistingCol(
						randCol,
						genErrStr(colsInConstraint, randColVal, error, sim,
								(char) -1));
			} else {
				String newConsVal = simConsVals.get(rand.nextInt(simConsVals
						.size()));

				if (newConsVal.trim().equals(randColVal.trim()))
					error.modifyValForExistingCol(
							randCol,
							genErrStr(colsInConstraint, randColVal, error, sim,
									(char) -1));
				else
					error.modifyValForExistingCol(randCol, newConsVal.trim());
			}

		} else if (type.getType() == ErrorType.Type.IN_DOMAIN) {
			List<String> rand = idx.searchRand(randCol, 1);

			if (rand == null || rand.isEmpty()
					|| rand.get(0).trim().equals(randColVal.trim())) {
				error.modifyValForExistingCol(
						randCol,
						genErrStr(colsInConstraint, randColVal, error, sim,
								(char) -1));
			} else {
				error.modifyValForExistingCol(randCol, rand.get(0).trim());
			}

		} else if (type.getType() == ErrorType.Type.OUTSIDE_DOMAIN_SIMILAR) {
			error.modifyValForExistingCol(
					randCol,
					genErrStr(colsInConstraint, randColVal, error, sim,
							(char) -1));
		} else if (type.getType() == ErrorType.Type.OUTSIDE_DOMAIN) {
			error.modifyValForExistingCol(
					randCol,
					genErrStr(colsInConstraint, randColVal, error, sim,
							(char) -1));
		} else {
			List<String> specialChars = type.getSpecialChars();

			String s = specialChars.get(rand.nextInt(specialChars.size()));
			error.modifyValForExistingCol(
					randCol,
					genErrStr(colsInConstraint, randColVal, error, sim,
							s.charAt(0)));

		}

		emd.addErrorValForCol(randCol, error.getColsToVal().get(randCol));

		return error;
	}

	private String genErrStr(List<String> colsInConstraint, String randColVal,
			Record error, double sim, char sp) {

		String rStr = error.getRecordStr(colsInConstraint);
		double rStrLen = (double) rStr.length();
		int desiredLevDist = (int) ((1.0f - sim) * rStrLen);

		StringBuilder errStr = new StringBuilder(randColVal);

		if (desiredLevDist > errStr.length()) {
			// Add
			desiredLevDist = (int) ((rStrLen * (1d - sim)) / sim);

			if (desiredLevDist == 0) {
				desiredLevDist = 1;
			}

			char r = 'a';
			if (sp != (char) -1) {
				r = sp;
			} else {
				r = (char) (rand.nextInt(26) + 97);
			}

			String er = errStr.toString().trim()
					+ StringUtils.repeat(r, desiredLevDist);
			return er;

		} else {
			// Update
			if (desiredLevDist == 0) {
				desiredLevDist = 1;
			}

			int startIdx = 0;

			if (errStr.length() != desiredLevDist) {
				startIdx = rand.nextInt(errStr.length() - desiredLevDist);
			}

			while (desiredLevDist > 0) {
				char c = errStr.charAt(startIdx);

				char r = 'a';
				if (sp != (char) -1) {
					r = sp;
				} else {
					r = (char) (rand.nextInt(26) + 97);

					if (r == c) {
						r = (char) (r + 1);
					}
				}

				errStr.setCharAt(startIdx, r);
				startIdx++;
				desiredLevDist--;
			}

			return errStr.toString().trim();
		}
	}

	// TODO : Fix this so that errors cols are exact matches.
	private Query simConsqQuery(String cons, String consVal, double sim) {
		String[] field = new String[] { cons };

		MultiFieldQueryParser parser = new MultiFieldQueryParser(
				org.apache.lucene.util.Version.LUCENE_48, field,
				new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_48));

		if (consVal == null || "".equals(consVal.trim())) {
			consVal = "\" \"";
		}

		Query q = null;
		try {
			q = parser.parse(QueryParser.escape(consVal + "~ "
					+ (int) ((1.0d - sim) * (consVal.length()))));
		} catch (ParseException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

		// logger.log(ProdLevel.PROD, "Executing lucene query : " + q);
		return q;
	}

	private List<Record> purgeFilteredOut(List<Record> groundTruth,
			List<Record> filteredOut) {
		Set<Record> filteredOutSet = new HashSet<>(filteredOut);
		List<Record> newGt = new ArrayList<>();

		for (Record gt : groundTruth) {
			if (!filteredOutSet.contains(gt)) {
				newGt.add(gt);
			}
		}

		return newGt;
	}

	@Override
	public void addErrorsCumulative(List<ErrorType> types, int desiredTgtSize,
			int roughChunkSize, double[] percentageAnt,
			double[] percentageCons, String gtUrl, List<String> tgtOutUrls,
			String fdUrl, List<String> errMetadataUrls, char separator,
			char quoteChar, int[] numNonMatch) throws Exception {
		if (percentageCons == null || percentageCons.length == 0)
			return;

		Assert.assertTrue(isSorted(percentageAnt));
		Assert.assertTrue(isSorted(percentageCons));

		// Gather some metadata about dataset with highest errors.
		double pAnt = percentageAnt[percentageAnt.length - 1];
		double pCons = percentageCons[percentageCons.length - 1];
		String tgtOutUrl = tgtOutUrls.get(tgtOutUrls.size() - 1);
		String errMetadataUrl = errMetadataUrls.get(errMetadataUrls.size() - 1);

		logger.log(ProdLevel.PROD, "\nCreating : " + tgtOutUrl);

		// Build tgt dataset with the highest errors.
		TargetDataset hErrDataset = addErrorsRandom(types, desiredTgtSize,
				roughChunkSize, pAnt, pCons, gtUrl, tgtOutUrl, "", fdUrl,
				errMetadataUrl, separator, quoteChar);
		// TargetDataset hErrDataset =
		// datasetService.loadTargetDataset(tgtOutUrl,
		// "", fdUrl, separator, quoteChar);

		logger.log(ProdLevel.PROD, "Created : " + tgtOutUrl);

		// Get objects related to the dataset with the highest errors.
		List<Constraint> constraints = hErrDataset.getConstraints();

		dummyErrorRecordId = desiredTgtSize + 1;

		logger.log(ProdLevel.PROD, "\nWorking backwards...");
		List<Record> innocuous = findInnocuousRecs(constraints,
				hErrDataset.getRecords(), datasetService.loadErrMetadata(
						errMetadataUrl, separator, quoteChar));

		// Now we create other tgt datasets (except the last one with the
		// highest errors) using the last one.
		for (int i = percentageAnt.length - 2; i > -1; i--) {
			createTgtDatasetsCumulative(constraints, innocuous,
					percentageCons[i], percentageCons[i + 1],
					errMetadataUrls.get(i), errMetadataUrls.get(i + 1),
					tgtOutUrls.get(i), tgtOutUrls.get(i + 1), fdUrl,
					desiredTgtSize, separator, quoteChar, numNonMatch[i]);
		}
	}

	private void createTgtDatasetsCumulative(List<Constraint> constraints,
			List<Record> innocuous, double pConsCurr, double pConsPrev,
			String errMetadataUrlCurr, String errMetadataUrlPrev,
			String tgtOutUrlCurr, String tgtOutUrlPrev, String fdUrl,
			int desiredTgtSize, char separator, char quoteChar, int numNonMatch) {

		logger.log(ProdLevel.PROD, "\nCreating : " + tgtOutUrlCurr
				+ ", From : " + tgtOutUrlPrev);

		TargetDataset prevDataset = datasetService.loadTargetDataset(
				tgtOutUrlPrev, "", fdUrl, separator, quoteChar);

		List<ErrorMetadata> prevEmds = datasetService.loadErrMetadata(
				errMetadataUrlPrev, separator, quoteChar);

		int numErrors = (int) ((double) desiredTgtSize * pConsCurr);

		double fract = pConsCurr / pConsPrev;
		int emdTrimLength = (int) (prevEmds.size() * fract);

		if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.CUMUL_SORT_ASC) {
			prevEmds = sortByViolSize(prevEmds, prevDataset);
		} else if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.CUMUL_SHUFFLE) {
			shuffle(prevEmds);
		} else if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.CUMUL_SORT_SWAP) {
			prevEmds = sortByViolSize(prevEmds, prevDataset);
			swap(prevEmds, 0.2f);
		} else if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.CUMUL_SORT_MATCH_DESC) {
			prevEmds = sortByMatches(prevEmds, prevDataset);
		} else if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.CUMUL_SORT_SWAP_MATCH) {
			prevEmds = sortByMatches(prevEmds, prevDataset);
			prevEmds = swapEmdChunk(prevEmds, emdTrimLength, numNonMatch);
		}

		List<ErrorMetadata> currentEmds = prevEmds.subList(0, emdTrimLength);

		List<ErrorMetadata> excessEmds = prevEmds.subList(emdTrimLength,
				prevEmds.size());

		printAvgSimilarity(currentEmds, prevDataset);

		Assert.assertEquals(currentEmds.size() + excessEmds.size(),
				prevEmds.size());
		List<Violations> prevViols = calcViolsReverseExcess(constraints,
				prevDataset, excessEmds);

		// Removing excess emds could have resulted in useless emds among the
		// current.
		pruneIrrelevantEmds(currentEmds, prevViols);

		trimOrAddCurrentViols(constraints, currentEmds, prevViols, numErrors);

		List<Record> tgtRecs = createTgtRecsCumulative(currentEmds, prevViols,
				innocuous, prevDataset, desiredTgtSize);

		saveErrorMetadata(currentEmds, errMetadataUrlCurr, separator, quoteChar);

		datasetService
				.saveDataset(tgtRecs, tgtOutUrlCurr, separator, quoteChar);

		logger.log(ProdLevel.PROD, "Created : " + tgtOutUrlCurr + ", From : "
				+ tgtOutUrlPrev);

	}

	private List<ErrorMetadata> swapEmdChunk(List<ErrorMetadata> prevEmds,
			int emdTrimLength, int numNonMatch) {

		List<ErrorMetadata> currentEmds = prevEmds.subList(0, emdTrimLength);
		List<ErrorMetadata> excessEmds = prevEmds.subList(emdTrimLength,
				prevEmds.size());

		int numToRemove = numNonMatch;
		int excessEmdsSize = excessEmds.size();
		int toTransfer = Math.min(excessEmdsSize, numToRemove);

		List<ErrorMetadata> toReturn = new ArrayList<>();

		List<ErrorMetadata> exTransfer = excessEmds.subList(excessEmds.size()
				- toTransfer, excessEmds.size());
		List<ErrorMetadata> exTrim = excessEmds.subList(0, excessEmds.size()
				- toTransfer);

		List<ErrorMetadata> cTransfer = currentEmds.subList(currentEmds.size()
				- toTransfer, currentEmds.size());
		List<ErrorMetadata> cOk = currentEmds.subList(0, currentEmds.size()
				- toTransfer);

		toReturn.addAll(cOk);
		toReturn.addAll(exTransfer);
		toReturn.addAll(exTrim);
		toReturn.addAll(cTransfer);
		logger.log(ProdLevel.PROD, "exTransfer : " + exTransfer.size() + ", "
				+ exTransfer);

		return toReturn;
	}

	/**
	 * Trimming away excess emds earlier could give rise to redundant emds
	 * within the currentEmds. We should remove these because they could
	 * influence precision, recall and F1.
	 * 
	 * @param currentEmds
	 * @param prevViols
	 */
	private void pruneIrrelevantEmds(List<ErrorMetadata> currentEmds,
			List<Violations> prevViols) {
		Set<Long> rids = new HashSet<>();
		for (Violations v : prevViols) {
			Collection<Record> recs = v.getViolMap().values();
			for (Record rec : recs) {
				rids.add(rec.getId());
			}
		}
		List<ErrorMetadata> toRem = new ArrayList<>();

		for (ErrorMetadata emd : currentEmds) {
			if (!rids.contains(emd.getTid())) {
				toRem.add(emd);
			}
		}

		logger.log(ProdLevel.PROD, "Pruning current emds : " + toRem.size());
		currentEmds.removeAll(toRem);
	}

	private void printAvgSimilarity(List<ErrorMetadata> prevEmds,
			TargetDataset prevDataset) {
		double totalSim = 0d;
		List<Constraint> constraints = prevDataset.getConstraints();
		List<List<String>> constraintCols = new ArrayList<>();

		for (Constraint constraint : constraints) {
			constraintCols.add(constraint.getColsInConstraint());
		}

		for (ErrorMetadata e : prevEmds) {
			Record r = prevDataset.getRecord(e.getTid());
			Map<List<String>, String> colsToAntsCons = new HashMap<>();
			Map<List<String>, String> colsToAntsConsOrig = new HashMap<>();

			for (List<String> constraintCol : constraintCols) {
				String s1 = r.getRecordStr(constraintCol);
				colsToAntsCons.put(constraintCol, s1);
			}

			Map<String, String> cToVOrig = e.getOrigColsToVal();

			for (Map.Entry<String, String> entry : cToVOrig.entrySet()) {
				r.modifyValForExistingCol(entry.getKey(), entry.getValue());
			}

			for (List<String> constraintCol : constraintCols) {
				String s2 = r.getRecordStr(constraintCol);
				colsToAntsConsOrig.put(constraintCol, s2);
			}

			Map<String, String> cToV = e.getErrorsColsToVal();

			for (Map.Entry<String, String> entry : cToV.entrySet()) {
				r.modifyValForExistingCol(entry.getKey(), entry.getValue());
			}

			double sim = 0d;
			double numDiff = 0d;
			for (Map.Entry<List<String>, String> c : colsToAntsCons.entrySet()) {
				String err = c.getValue();
				String orig = colsToAntsConsOrig.get(c.getKey());
				int levDist = DistanceMeasures.getLevDistance(err, orig);
				double simForCons = calcSim(levDist, err.length(),
						orig.length());

				if (simForCons > Config.FLOAT_EQUALIY_EPSILON) {
					sim += simForCons;
					numDiff++;
				}
			}

			sim = sim / numDiff;
			Pair<ErrorMetadata, Double> p = new Pair<>();
			p.setO1(e);
			p.setO2(sim);
			totalSim += sim;
		}

		logger.log(ProdLevel.PROD, "AVG SIM: " + totalSim / prevEmds.size());

	}

	private double calcSim(int levDist, int s1Len, int s2Len) {
		double simForCons = 1.0f - (float) levDist
				/ (float) Math.max(s1Len, s2Len);
		return simForCons;
	}

	private List<ErrorMetadata> sortByMatches(List<ErrorMetadata> prevEmds,
			TargetDataset prevDataset) {
		List<Violations> orderedViols = repairService
				.orderViolations(prevDataset);
		Map<Constraint, Set<Long>> consToRecs = new HashMap<>();

		for (Violations v : orderedViols) {
			Collection<Record> recs = v.getViolMap().values();
			Set<Long> rids = new HashSet<>();

			for (Record r : recs) {
				rids.add(r.getId());
			}

			consToRecs.put(v.getConstraint(), rids);
		}

		Queue<Pair<ErrorMetadata, Double>> heap = new PriorityQueue<>(
				prevEmds.size(), new Comparator<Pair<ErrorMetadata, Double>>() {

					@Override
					public int compare(Pair<ErrorMetadata, Double> o1,
							Pair<ErrorMetadata, Double> o2) {
						double d1 = o1.getO2();
						double d2 = o2.getO2();
						return -1 * Double.compare(d1, d2);
					}
				});

		Map<Constraint, Set<String>> consToCols = new HashMap<>();
		for (Constraint c : consToRecs.keySet()) {
			consToCols.put(c, new HashSet<>(c.getColsInConstraint()));
		}

		for (ErrorMetadata emd : prevEmds) {
			for (Constraint c : consToRecs.keySet()) {
				Set<String> cols = consToCols.get(c);
				Set<Long> rids = consToRecs.get(c);

				if (rids.contains(emd.getTid())) {
					Set<String> errCols = emd.getErrorsColsToVal().keySet();
					boolean acceptCons = false;

					for (String errCol : errCols) {
						if (cols.contains(errCol)) {
							acceptCons = true;
							break;
						}
					}

					if (acceptCons) {
						double s = getEmdSim(emd,
								prevDataset.getRecord(emd.getTid()), c);

						Pair<ErrorMetadata, Double> p = new Pair<>();
						p.setO1(emd);
						p.setO2(s);
						heap.offer(p);

						break;
					}
				}
			}
		}

		List<ErrorMetadata> sortedEmds = new ArrayList<>();
		while (!heap.isEmpty()) {
			Pair<ErrorMetadata, Double> p = heap.poll();
			ErrorMetadata emd = p.getO1();
			sortedEmds.add(emd);

			// logger.log(ProdLevel.PROD,
			// "heap emd: " + emd + ", avgsim :" + p.getO2());
		}

		return sortedEmds;
	}

	private double getEmdSim(ErrorMetadata e, Record r, Constraint constraint) {

		String err = r.getRecordStr(constraint.getColsInConstraint());

		Map<String, String> origToVal = e.getOrigColsToVal();

		for (Map.Entry<String, String> entry : origToVal.entrySet()) {
			r.modifyValForExistingCol(entry.getKey(), entry.getValue());
		}

		String orig = r.getRecordStr(constraint.getColsInConstraint());

		Map<String, String> errToVal = e.getErrorsColsToVal();

		for (Map.Entry<String, String> entry : errToVal.entrySet()) {
			r.modifyValForExistingCol(entry.getKey(), entry.getValue());
		}

		int levDist = DistanceMeasures.getLevDistance(err, orig);
		double sim = calcSim(levDist, err.length(), orig.length());
		return sim;
	}

	private void swap(List<ErrorMetadata> a, float f) {
		int s = a.size();
		int N = (int) (a.size() * f);
		for (int i = 0; i < N; i++) {
			int r1 = rand.nextInt(s);
			int r2 = rand.nextInt(s);

			ErrorMetadata swap = copyErrorMetadata(a.get(r1));
			a.set(r1, copyErrorMetadata(a.get(r2)));
			a.set(r2, swap);
		}
	}

	private List<ErrorMetadata> sortByViolSize(List<ErrorMetadata> prevEmds,
			TargetDataset prevDataset) {
		List<ErrorMetadata> sortedEmds = new ArrayList<>();

		List<Record> recs = prevDataset.getRecords();
		List<Constraint> constraints = prevDataset.getConstraints();
		Set<Long> emdIds = new HashSet<>();
		Map<Long, ErrorMetadata> emdIdToObj = new HashMap<>();

		for (ErrorMetadata e : prevEmds) {
			emdIdToObj.put(e.getTid(), e);
			emdIds.add(e.getTid());
		}

		Multimap<String, Long> antsToEmd = HashMultimap.create();

		for (Constraint constraint : constraints) {
			Violations v = repairService.calcViolations(recs, constraint);
			Multimap<String, Record> vMap = v.getViolMap();

			for (String ants : vMap.keySet()) {
				Collection<Record> rs = vMap.get(ants);
				for (Record r : rs) {
					if (emdIds.contains(r.getId())) {
						antsToEmd.put(ants, r.getId());
					}
				}
			}

		}

		Queue<Collection<Long>> heap = new PriorityQueue<>(antsToEmd.keySet()
				.size(), new Comparator<Collection<Long>>() {

			@Override
			public int compare(Collection<Long> o1, Collection<Long> o2) {
				return Integer.compare(o1.size(), o2.size());
			}

		});

		// Heap sort
		for (String ants : antsToEmd.keySet()) {
			heap.offer(antsToEmd.get(ants));
		}

		while (!heap.isEmpty()) {
			Collection<Long> eIds = heap.poll();

			for (long eId : eIds) {
				sortedEmds.add(emdIdToObj.get(eId));
			}

		}

		return sortedEmds;
	}

	private void shuffle(List<ErrorMetadata> a) {

		int N = a.size();
		for (int i = 0; i < N; i++) {
			// choose index uniformly in [i, N-1]
			int r = i + rand.nextInt(N - i);
			ErrorMetadata swap = copyErrorMetadata(a.get(r));
			a.set(r, copyErrorMetadata(a.get(i)));
			a.set(i, swap);
		}
	}

	private List<Violations> calcViolsReverseExcess(
			List<Constraint> constraints, TargetDataset tgtDataset,
			List<ErrorMetadata> excessEmds) {
		List<Violations> prevViols = new ArrayList<>();

		// The excess emds are those that were removed from the prev emds.
		for (ErrorMetadata emd : excessEmds) {
			Map<String, String> oCtoV = emd.getOrigColsToVal();
			// Reverse the excess emds.
			for (Map.Entry<String, String> e : oCtoV.entrySet()) {
				tgtDataset.getRecord(emd.getTid()).modifyValForExistingCol(
						e.getKey(), e.getValue());
			}

		}

		for (Constraint constraint : constraints) {
			prevViols.add(repairService.calcViolations(tgtDataset.getRecords(),
					constraint));
		}

		return prevViols;
	}

	private List<Record> createTgtRecsCumulative(List<ErrorMetadata> emds,
			List<Violations> viols, List<Record> innocuous,
			TargetDataset tDataset, int desiredTgtSize) {
		Set<Long> booked = getEmdIds(emds);
		Set<Record> violsToAddS = new HashSet<>();

		for (Violations v : viols) {
			Multimap<String, Record> vMap = v.getViolMap();
			for (String key : vMap.keySet()) {
				Collection<Record> recs = vMap.get(key);

				for (Record r : recs) {
					if (!booked.contains(r.getId())) {
						violsToAddS.add(r);
					}
				}
			}
		}

		List<Record> violsToAdd = new ArrayList<>(violsToAddS);

		List<Record> reassigned = new ArrayList<>();

		for (long i = 0; i < desiredTgtSize; i++) {
			if (booked.contains(i + 1)) {
				reassigned.add(tDataset.getRecord(i + 1));
			} else {
				if (!violsToAdd.isEmpty()) {
					// Add viol.
					Record vToA = violsToAdd.remove(violsToAdd.size() - 1);
					Record r = new Record(i + 1);
					r.setColsToVal(new LinkedHashMap<>(vToA.getColsToVal()));
					reassigned.add(r);
				} else {
					// Add innocuous.
					Record toA = innocuous.get(rand.nextInt(innocuous.size()));
					Record r = new Record(i + 1);
					r.setColsToVal(new LinkedHashMap<>(toA.getColsToVal()));
					reassigned.add(r);
				}
			}
		}

		return reassigned;
	}

	private boolean isSorted(double[] arr) {
		double oldD = Double.MIN_VALUE;

		for (double d : arr) {
			if (d >= oldD) {
				oldD = d;
			} else {
				return false;
			}
		}

		return true;
	}

	private void trimOrAddCurrentViols(List<Constraint> constraints,
			List<ErrorMetadata> currentEmds, List<Violations> currentViols,
			int numErrors) {
		int count = 0;
		Map<Constraint, Multimap<String, Record>> constraintToViolMap = new HashMap<>();
		Map<Constraint, List<String>> constraintToAntCols = new HashMap<>();

		for (Violations v : currentViols) {
			count += v.getViolMap().size();
			constraintToViolMap.put(v.getConstraint(), v.getViolMap());
			constraintToAntCols.put(v.getConstraint(), v.getConstraint()
					.getAntecedentCols());
		}

		if (count < numErrors) {
			logger.log(ProdLevel.PROD, "Add errors.");

			List<RecordViolInfo> addable = findRedundantRecords(constraints,
					currentEmds, currentViols);

			while (count < numErrors) {
				if (addable == null || addable.isEmpty()) {
					break;
				}

				RecordViolInfo rvi = addable.get(rand.nextInt(addable.size()));
				Record copy = copyRecord(rvi.getRecord());
				Collection<Constraint> cons = rvi.getConstraints();

				for (Constraint con : cons) {
					Multimap<String, Record> violMap = constraintToViolMap
							.get(con);
					List<String> ants = constraintToAntCols.get(con);
					violMap.put(copy.getRecordStr(ants), copy);
				}

				count += rvi.getConstraints().size();
			}
		} else if (count > numErrors) {
			logger.log(ProdLevel.PROD, "Delete errors.");

			List<RecordViolInfo> deletable = findRedundantRecords(constraints,
					currentEmds, currentViols);

			while (count > numErrors) {

				if (deletable == null || deletable.isEmpty()) {
					break;
				}

				RecordViolInfo rvi = deletable.remove(deletable.size() - 1);
				Collection<Constraint> cons = rvi.getConstraints();

				for (Constraint con : cons) {
					Multimap<String, Record> violMap = constraintToViolMap
							.get(con);
					List<String> ants = constraintToAntCols.get(con);
					violMap.remove(rvi.getRecord().getRecordStr(ants),
							rvi.getRecord());
				}

				count -= rvi.getConstraints().size();
			}
		}
	}

	/**
	 * There are recs which are not in the current emd list. Removing any 1
	 * record in the list will lower the num violations by 1 only (not 2, which
	 * can happen if the violation is completely corrected OR if the record is
	 * involved in multiple constraints). Also, adding any 1 record should
	 * increase the num violations by 1 only (not more than 1, which can happen
	 * if the record is involved in multiple constraints).
	 * 
	 * @param currentEmds
	 * @param currentViols
	 * @return
	 */
	private List<RecordViolInfo> findRedundantRecords(
			List<Constraint> constraints, List<ErrorMetadata> currentEmds,
			List<Violations> currentViols) {
		List<RecordViolInfo> redundant = new ArrayList<>();
		Set<Long> emdIds = getEmdIds(currentEmds);
		Multimap<Record, Constraint> redundantRecToConstraints = HashMultimap
				.create();

		for (Violations v : currentViols) {
			Constraint c = v.getConstraint();
			Multimap<String, Record> violMap = v.getViolMap();

			for (String key : violMap.keySet()) {
				Collection<Record> recs = violMap.get(key);

				boolean nonEmdAdded = false;

				for (Record r : recs) {
					if (emdIds.contains(r.getId())) {
						continue;
					} else if (nonEmdAdded) {
						redundantRecToConstraints.put(r, c);
					} else {
						// Even though current record is not in emdIds, we count
						// this record as being non-redundant (otherwise the
						// violation chunk would disappear if we removed this
						// record). However, subsequent non-emd records are
						// redundant.
						nonEmdAdded = true;
					}

				}
			}

		}

		for (Record r : redundantRecToConstraints.keySet()) {
			RecordViolInfo rvi = new RecordViolInfo();
			rvi.setRecord(r);
			rvi.setConstraints(redundantRecToConstraints.get(r));
			redundant.add(rvi);
		}

		Collections.sort(redundant, new Comparator<RecordViolInfo>() {

			@Override
			public int compare(RecordViolInfo o1, RecordViolInfo o2) {
				if (o1.getConstraints().size() > o2.getConstraints().size()) {
					return -1;
				} else if (o1.getConstraints().size() < o2.getConstraints()
						.size()) {
					return 1;
				} else {
					return 0;
				}
			}
		});

		return redundant;
	}

	class RecordViolInfo {
		private Record record;
		private Collection<Constraint> constraints;

		public RecordViolInfo() {
			this.constraints = new ArrayList<>();
		}

		public Record getRecord() {
			return record;
		}

		public void setRecord(Record record) {
			this.record = record;
		}

		public void addConstraint(Constraint constraint) {
			this.constraints.add(constraint);
		}

		public Collection<Constraint> getConstraints() {
			return constraints;
		}

		public void setConstraints(Collection<Constraint> constraints) {
			this.constraints = constraints;
		}

		@Override
		public String toString() {
			return "RecordViolInfo [record=" + record + ", constraints="
					+ constraints + "]";
		}

	}

	private Set<Long> getEmdIds(List<ErrorMetadata> emds) {
		Set<Long> emdIds = new HashSet<>();

		for (ErrorMetadata emd : emds) {
			long tid = emd.getTid();
			emdIds.add(tid);
		}

		return emdIds;
	}

	@Override
	public void addErrorsCumulativeIncTuples(List<ErrorType> types,
			int[] desiredTgtSizes, int roughChunkSizeSmallest,
			double percentageAnt, double percentageCons, String gtUrl,
			List<String> tgtOutUrls, String fdUrl,
			List<String> errMetadataUrls, char separator, char quoteChar,
			int[] numNonMatch) throws Exception {
		// Gather some metadata about dataset with highest errors.
		String tgtOutUrl = tgtOutUrls.get(0);
		String errMetadataUrl = errMetadataUrls.get(0);

		logger.log(ProdLevel.PROD, "\nCreating : " + tgtOutUrl);

		// Build tgt dataset with the lowest errors.
		TargetDataset lErrDataset = addErrorsRandom(types, desiredTgtSizes[0],
				roughChunkSizeSmallest, percentageAnt, percentageCons, gtUrl,
				tgtOutUrl, "", fdUrl, errMetadataUrl, separator, quoteChar);
		// TargetDataset lErrDataset =
		// datasetService.loadTargetDataset(tgtOutUrl,
		// "", fdUrl, separator, quoteChar);

		logger.log(ProdLevel.PROD, "Created : " + tgtOutUrl);

		// Get objects related to the dataset with the highest errors.
		List<Constraint> constraints = lErrDataset.getConstraints();

		dummyErrorRecordId = desiredTgtSizes[desiredTgtSizes.length - 1] + 1;

		List<Record> innocuous = findInnocuousRecs(constraints,
				lErrDataset.getRecords(), datasetService.loadErrMetadata(
						errMetadataUrl, separator, quoteChar));

		for (int i = 1; i < desiredTgtSizes.length; i++) {
			createTgtDatasetsCumulativeIncTuples(constraints, innocuous,
					percentageCons, errMetadataUrls.get(i),
					errMetadataUrls.get(i - 1), tgtOutUrls.get(i),
					tgtOutUrls.get(i - 1), fdUrl, desiredTgtSizes[i],
					separator, quoteChar, numNonMatch[i], numNonMatch[0]);
		}
	}

	private void createTgtDatasetsCumulativeIncTuples(
			List<Constraint> constraints, List<Record> innocuous,
			double percentageCons, String errMetadataUrlCurr,
			String errMetadataUrlPrev, String tgtOutUrlCurr,
			String tgtOutUrlPrev, String fdUrl, int desiredTgtSize,
			char separator, char quoteChar, int numNonMatch,
			int numNonMatchSmallest) {
		logger.log(ProdLevel.PROD, "\nCreating : " + tgtOutUrlCurr
				+ ", From : " + tgtOutUrlPrev);

		TargetDataset prevDataset = datasetService.loadTargetDataset(
				tgtOutUrlPrev, "", fdUrl, separator, quoteChar);

		List<ErrorMetadata> prevEmds = datasetService.loadErrMetadata(
				errMetadataUrlPrev, separator, quoteChar);

		prevEmds = sortByMatches(prevEmds, prevDataset);

		List<ErrorMetadata> nonMatchEmds = prevEmds.subList(prevEmds.size()
				- numNonMatchSmallest, prevEmds.size());

		// logger.log(ProdLevel.PROD, "\nprevEmds, " + prevEmds.size() + " : "
		// + prevEmds);

		List<Violations> prevViols = new ArrayList<>();

		int numDesiredErrors = (int) ((double) desiredTgtSize * percentageCons);
		int numCurrErrors = 0;

		for (Constraint constraint : constraints) {
			Violations v = repairService.calcViolations(
					prevDataset.getRecords(), constraint);
			numCurrErrors += v.getViolMap().size();
			prevViols.add(v);
		}

		List<Record> tgtRecs = prevDataset.getRecords();
		dummyErrorRecordId = tgtRecs.size() + 1;
		List<Record> newRecs = addViols(constraints, prevDataset, prevEmds,
				prevViols, numDesiredErrors - numCurrErrors, nonMatchEmds,
				numNonMatch);
		tgtRecs.addAll(newRecs);
		int numInnocuous = desiredTgtSize - tgtRecs.size();

		for (int i = 0; i < numInnocuous; i++) {
			Record toAdd = innocuous.get(rand.nextInt(innocuous.size()));
			// logger.log(ProdLevel.PROD,
			// "Adding innocuous : " + toAdd.getColsToVal());
			tgtRecs.add(copyRecord(toAdd));
		}

		saveErrorMetadata(prevEmds, errMetadataUrlCurr, separator, quoteChar);

		datasetService
				.saveDataset(tgtRecs, tgtOutUrlCurr, separator, quoteChar);

		logger.log(ProdLevel.PROD, "Created : " + tgtOutUrlCurr + ", From : "
				+ tgtOutUrlPrev);

	}

	private List<Record> addViols(List<Constraint> constraints,
			TargetDataset tgtDataset, List<ErrorMetadata> prevEmds,
			List<Violations> prevViols, int numToAddErrors,
			List<ErrorMetadata> nonMatchEmds, int numNonMatch) {
		Set<Long> emdIds = new HashSet<>();
		Set<Long> nonMatchEmdIds = getEmdIds(nonMatchEmds);
		List<ErrorMetadata> toAddEmds = new ArrayList<>();
		Map<Long, ErrorMetadata> emdIdToObj = new HashMap<>();
		Map<Long, ErrorMetadata> nonEmdIdToObj = new HashMap<>();

		for (ErrorMetadata e : prevEmds) {
			emdIdToObj.put(e.getTid(), e);
			emdIds.add(e.getTid());

			if (nonMatchEmdIds.contains(e.getTid()))
				nonEmdIdToObj.put(e.getTid(), e);
		}

		Multimap<Record, Constraint> recToConstraints = HashMultimap.create();

		for (Violations v : prevViols) {
			Collection<Record> recs = v.getViolMap().values();

			for (Record rec : recs) {
				recToConstraints.put(rec, v.getConstraint());
			}
		}

		int count = 0;
		List<Record> toAdd = new ArrayList<>();

		for (int i = 0; i < numNonMatch; i++) {
			ErrorMetadata nmEmd = nonMatchEmds.get(rand.nextInt(nonMatchEmds
					.size()));
			Record r = tgtDataset.getRecord(nmEmd.getTid());
			Record copy = copyRecord(r);

			ErrorMetadata emdCopy = copyErrorMetadata(nmEmd);
			emdCopy.setTid(copy.getId());
			toAddEmds.add(emdCopy);
			toAdd.add(copy);
			count += recToConstraints.get(r).size();
		}

		List<Record> vRecs = new ArrayList<>(recToConstraints.keySet());

		while (count < numToAddErrors) {
			Record viol = vRecs.get(rand.nextInt(vRecs.size()));
			Record copy = copyRecord(viol);

			if (emdIds.contains(viol.getId())) {
				ErrorMetadata emd = emdIdToObj.get(viol.getId());
				ErrorMetadata emdCopy = copyErrorMetadata(emd);
				emdCopy.setTid(copy.getId());
				toAddEmds.add(emdCopy);
			}

			toAdd.add(copy);
			count += recToConstraints.get(viol).size();
		}

		prevEmds.addAll(toAddEmds);
		return toAdd;

	}
}