/**
 */
package data.cleaning.core.service.dataset.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.errgen.impl.ErrorMetadata;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.RepairService;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.DebugLevel;
import data.cleaning.core.utils.DistanceMeasures;
import data.cleaning.core.utils.HashMapIndex;
import data.cleaning.core.utils.ProdLevel;

@Service("datasetService")
public class DatasetServiceImpl implements DatasetService {
	@Autowired
	@Qualifier(value = "repairService")
	protected RepairService repairService;

	private static final Logger logger = Logger
			.getLogger(DatasetServiceImpl.class);
	private Gson gson;
	private long dummyErrorRecordId;
	private Random rand;

	public DatasetServiceImpl() {
		this.gson = new Gson();
		this.rand = new Random(Config.SEED);
	}

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

	@Override
	public TargetDataset loadTargetDataset(String tgtUrl, String tgtName,
			String fdUrl, char separator, char quoteChar) {
		return (TargetDataset) loadDataset(tgtUrl, tgtName, fdUrl,
				DatasetType.TARGET, separator, quoteChar);
	}

	@Override
	public List<ErrorMetadata> loadErrMetadata(String errMetadataUrl,
			char separator, char quoteChar) {
		List<ErrorMetadata> emds = null;

		InputStream is = null;
		try {
			is = new FileInputStream(errMetadataUrl);
			emds = constructErrsMetadata(ByteStreams.toByteArray(is),
					separator, quoteChar);
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}
		}

		return emds;
	}

	@Override
	public MasterDataset loadMasterDataset(String mUrl, String mName,
			String fdUrl, long targetId, char separator, char quoteChar) {

		MasterDataset mDataset = (MasterDataset) loadDataset(mUrl, mName,
				fdUrl, DatasetType.MASTER, separator, quoteChar);
		mDataset.setTid(targetId);

		return mDataset;
	}

	@Override
	public GroundTruthDataset loadGroundTruthDataset(String gtUrl,
			String gtName, String fdUrl, long targetId, char separator,
			char quoteChar) {

		GroundTruthDataset gtDataset = (GroundTruthDataset) loadDataset(gtUrl,
				gtName, fdUrl, DatasetType.GROUND_TRUTH, separator, quoteChar);
		gtDataset.setTid(targetId);

		return gtDataset;
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
								if (cols.get(i) == null) {
									logger.log(ProdLevel.PROD, "rid : " + count
											+ " has a null col."
											+ " colnum #: " + i
											+ ", and colmap is :" + cols);
								}

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

	private List<ErrorMetadata> constructErrsMetadata(byte[] bytes,
			char separator, char quoteChar) {
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

			Map<Long, ErrorMetadata> tIdToEmd = new HashMap<>();

			long tid = 0;
			String col = "";
			String origVal = "";
			String errVal = "";
			long gtId = 0;
			String val = "";
			int count = 0;

			while ((nextLine = reader.readNext()) != null) {
				if (count > 0) {
					for (int i = 0; i < nextLine.length; i++) {
						val = nextLine[i].trim();
						if (i == 0) {
							tid = Long.parseLong(val);
						} else if (i == 1) {
							col = val;
						} else if (i == 2) {
							origVal = val;
						} else if (i == 3) {
							errVal = val;
						} else {
							gtId = Long.parseLong(val);
						}
					}

					if (tIdToEmd.containsKey(tid)) {
						tIdToEmd.get(tid).addOrigValForCol(col, origVal);
						tIdToEmd.get(tid).addErrorValForCol(col, errVal);
					} else {
						ErrorMetadata et = new ErrorMetadata();
						et.setTid(tid);
						et.setGtId(gtId);
						et.addOrigValForCol(col, origVal);
						et.addErrorValForCol(col, errVal);
						tIdToEmd.put(tid, et);
					}
				}
				count++;

			}

			return new ArrayList<>(tIdToEmd.values());
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}
		}

		return null;
	}

	@Override
	public InfoContentTable calcInfoContentTable(Constraint constraint,
			MasterDataset mDataset) {

		logger.log(ProdLevel.PROD, "calc info content table");

		List<Record> records = mDataset.getRecords();
		List<String> colNames = constraint.getColsInConstraint();

		// Gather some statistics.
		Map<String, Set<String>> colNameToDomain = new HashMap<>();
		Map<String, Multiset<String>> colNameToValFreq = new HashMap<>();
		Multiset<String> patternValFreq = HashMultiset.create();

		for (int i = 0; i < colNames.size(); i++) {
			String colName = colNames.get(i);
			Set<String> domain = new HashSet<String>();
			Multiset<String> valFreq = HashMultiset.create();

			// Active domain.
			for (Record rec : records) {

				// Doing this here for optimization purposes.
				if (i == 0) {
					patternValFreq.add(rec.getRecordStr(colNames));
				}

				String val = rec.getColsToVal().get(colName);
				domain.add(val);
				valFreq.add(val);
			}

			// Inactive domain. Dummy value.
			domain.add("*");
			valFreq.add("*");
			colNameToDomain.put(colName, domain);
			colNameToValFreq.put(colName, valFreq);
		}

		return calcInfoContentTable(constraint, mDataset, colNameToDomain,
				colNameToValFreq, patternValFreq);
	}

	private InfoContentTable calcInfoContentTable(Constraint constraint,
			MasterDataset mDataset, Map<String, Set<String>> colNameToDomain,
			Map<String, Multiset<String>> colNameToValFreq,
			Multiset<String> patternValFreq) {
		Map<String, Double> cacheCellInfoContent = new HashMap<>();

		InfoContentTable table = new InfoContentTable();
		List<Record> records = mDataset.getRecords();
		List<String> colNames = constraint.getColsInConstraint();
		List<String> ants = constraint.getAntecedentCols();
		List<String> cons = constraint.getConsequentCols();
		BiMap<Integer, String> colIdToName = HashBiMap.create();

		for (int i = 0; i < colNames.size(); i++) {
			colIdToName.put(i, colNames.get(i));
		}

		int r = records.size();
		int c = colNames.size();
		double[][] data = new double[r][c];

		HashMapIndex hIdx = new HashMapIndex();
		hIdx.buildIndex(records, colNames);

		for (int col = 0; col < c; col++) {
			String colName = colIdToName.get(col);
			boolean isColAntecedent = ants.contains(colName);

			for (int row = 0; row < r; row++) {

				Record origRec = records.get(row);
				Map<String, String> origColsToVal = origRec.getColsToVal();

				Multiset<String> valFreq = colNameToValFreq.get(colName);

				String cacheKey = "v"
						+ valFreq.count(origColsToVal.get(colName))
						+ ", pv"
						+ patternValFreq.count(origRec.getRecordStr(constraint
								.getColsInConstraint()));

				if (cacheCellInfoContent.containsKey(cacheKey)) {
					data[row][col] = cacheCellInfoContent.get(cacheKey);
				} else {
					Set<String> satisfyFD = null;

					if (isColAntecedent) {
						List<String> andQueryCols = new ArrayList<>(ants);
						andQueryCols.remove(colName);
						List<String> negationQueryCols = new ArrayList<>();
						negationQueryCols.add(colName);
						negationQueryCols.addAll(cons);
						satisfyFD = new HashSet<>(colNameToDomain.get(colName));

						Set<String> doesntSatisfyFD = null;
						doesntSatisfyFD = hIdx.searchCol(origRec, andQueryCols,
								negationQueryCols, colName);

						// Complexity : O(|dontSatisfyFD|)
						satisfyFD.removeAll(doesntSatisfyFD);

					} else {
						Set<Integer> res = hIdx.search(origRec, ants);

						if (res.size() > 1) {
							satisfyFD = new HashSet<>();
							satisfyFD.add(origColsToVal.get(colName));
						} else {
							satisfyFD = new HashSet<>(
									colNameToDomain.get(colName));
						}

					}

					double b = satisfyFD.size();
					double oldP = 1d / b;

					for (String sat : satisfyFD) {

						// Comment below if you want to use the measure in
						// Arenas
						// and Libkin '05
						double weight = (double) valFreq.count(sat)
								/ (double) (r + 1);

						double p = oldP * weight;

						data[row][col] += p * (Math.log(1 / p) / Math.log(2d));
					}

					cacheCellInfoContent.put(cacheKey, data[row][col]);
				}

			}

		}

		table.setData(data);
		table.setColIdToName(colIdToName);
		removeInfoContentUsingHistory(table, mDataset);

		return table;
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

	@Override
	public void saveDataset(List<Record> records, String outputUrl,
			char separator, char quoteChar) {
		if (records == null || records.isEmpty())
			return;
		CSVWriter writer;
		try {
			if (quoteChar == (char) -1) {
				writer = new CSVWriter(new FileWriter(outputUrl), separator,
						CSVWriter.NO_QUOTE_CHARACTER);
			} else {
				writer = new CSVWriter(new FileWriter(outputUrl), separator,
						quoteChar);
			}

			List<String> cols = new ArrayList<>(records.get(0).getColsToVal()
					.keySet());

			String[] colsArr = cols.toArray(new String[cols.size()]);

			writer.writeNext(colsArr);

			for (int i = 0; i < records.size(); i++) {
				Record record = records.get(i);
				List<String> vals = new ArrayList<>();
				Map<String, String> cv = record.getColsToVal();

				for (Map.Entry<String, String> entry : cv.entrySet()) {
					vals.add(entry.getValue());
				}

				writer.writeNext(vals.toArray(new String[vals.size()]));
			}

			writer.close();
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

	}

	@SuppressWarnings("deprecation")
	public static IndexSearcher createSearcher(List<Record> records,
			List<String> cols) {
		RAMDirectory idx = new RAMDirectory();

		try {
			// Make an writer to create the index
			IndexWriter writer = new IndexWriter(idx, new IndexWriterConfig(
					Version.LUCENE_48, new StandardAnalyzer(Version.LUCENE_48)));

			for (Record record : records) {
				Document doc = new Document();

				doc.add(new Field("foo", record.getRecordStr(cols),
						Field.Store.YES, Field.Index.ANALYZED));

				writer.addDocument(doc);
			}

			writer.close();

			DirectoryReader r = DirectoryReader.open(idx);
			// Build an IndexSearcher using the in-memory index
			IndexSearcher searcher = new IndexSearcher(r);

			return searcher;
		} catch (IOException ioe) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + ioe.getMessage());
		}
		return null;
	}

	@Override
	public GroundTruthDataset constructGroundTruthDataset(String tgtUrl,
			String tgtName, String gtUrl, String gtName, String fdUrl,
			char separator, char quoteChar) {
		TargetDataset currentDataset = loadTargetDataset(tgtUrl, tgtName,
				fdUrl, separator, quoteChar);

		List<Record> groundTruth = currentDataset.getRecords();
		// These tuples were filtered out. They will not be in the groundTruth.
		List<Record> filteredOut = new ArrayList<>();

		for (int i = 0; i < currentDataset.getConstraints().size(); i++) {
			Constraint constraint = currentDataset.getConstraints().get(i);
			List<String> cons = Arrays.asList(constraint.getConsequent().split(
					","));

			Violations allViols = repairService.calcViolations(groundTruth,
					constraint);
			Multimap<String, Record> violMap = allViols.getViolMap();

			// logger.log(ProdLevel.PROD,
			// constraint + ", target dataset isSatisfied : "
			// + isSatisfied(constraint, groundTruth)
			// + ", num viols : " + allViols.getViolMap().size()
			// + ", num chunks : "
			// + allViols.getViolMap().keySet().size()
			// + ", viols : " + allViols);

			for (String key : violMap.keySet()) {
				List<Record> viols = new ArrayList<>(violMap.get(key));
				Multiset<String> counter = HashMultiset.create();
				// Violation record id whose consq has the highest support.
				int prototype = 0;
				int maxCount = Integer.MIN_VALUE;

				for (int j = 0; j < viols.size(); j++) {

					String consStr = viols.get(j).getRecordStr(cons);
					counter.add(consStr);

					if (counter.count(consStr) > maxCount) {
						prototype = j;
						maxCount = counter.count(consStr);
					}
				}

				for (int j = 0; j < viols.size(); j++) {
					if (j != prototype) {
						filteredOut.add(viols.get(j));
					}
				}
			}

			groundTruth = purgeFilteredOut(groundTruth, filteredOut);
		}

		saveDataset(groundTruth, gtUrl, separator, quoteChar);

		GroundTruthDataset gtDataset = (GroundTruthDataset) loadDataset(gtUrl,
				gtName, fdUrl, DatasetType.GROUND_TRUTH, separator, quoteChar);

		return gtDataset;
	}

	@Override
	public MasterDataset constructMasterDataset(String gtUrl, String mUrl,
			String mName, String tgtUrl, String tgtName, long targetId,
			String errMetadataUrl, String fdUrl, char separator,
			char quoteChar, float simThreshold, int numIncorrectMatches) {
		GroundTruthDataset gtDataset = loadGroundTruthDataset(gtUrl, "", fdUrl,
				targetId, separator, quoteChar);
		List<ErrorMetadata> emds = loadErrMetadata(errMetadataUrl, separator,
				quoteChar);
		TargetDataset tgtDataset = loadTargetDataset(tgtUrl, tgtName, fdUrl,
				separator, quoteChar);
		List<Record> groundTruth = gtDataset.getRecords();
		List<Record> mRecs = new ArrayList<>();
		Set<Long> mIds = new HashSet<>();

		for (ErrorMetadata emd : emds) {
			mIds.add(emd.getGtId());
		}

		for (long mId : mIds) {
			mRecs.add(groundTruth.get((int) mId - 1));
		}

		List<ErrorMetadata> bstEmds = getEmdsBelowSimThreshold(emds,
				tgtDataset, simThreshold);

		List<Constraint> constraints = tgtDataset.getConstraints();
		Set<String> unionAntecendents = new HashSet<>();

		for (Constraint constraint : constraints) {
			Set<String> ants = new HashSet<>(constraint.getAntecedentCols());
			unionAntecendents.addAll(ants);
		}

		addIncorrectMatches(constraints, mRecs, bstEmds, tgtDataset,
				new ArrayList<>(unionAntecendents), numIncorrectMatches);

		saveDataset(mRecs, mUrl, separator, quoteChar);

		MasterDataset mDataset = loadMasterDataset(mUrl, mName, fdUrl,
				gtDataset.getTid(), separator, quoteChar);

		return mDataset;
	}

	/**
	 * In a realistic scenario, some of the tgt recs will match master recs st
	 * suggested repairs to tgt vals will be incorrect.
	 * 
	 * @param mRecs
	 * @param bstEmds
	 * @param tgtDataset
	 * @param unionAntecendents
	 * @param numIncorrectMatches
	 */
	private void addIncorrectMatches(List<Constraint> constraints,
			List<Record> mRecs, List<ErrorMetadata> bstEmds,
			TargetDataset tgtDataset, List<String> unionAntecendents,
			int numIncorrectMatches) {
		dummyErrorRecordId = tgtDataset.getRecords().size() + mRecs.size() + 1;
		int numIncorrect = Math.min(bstEmds.size(), numIncorrectMatches);

		Map<Constraint, Map<String, String>> consToAntsCols = new HashMap<>();

		for (Constraint constraint : constraints) {
			List<String> ants = constraint.getAntecedentCols();
			List<String> cons = constraint.getConsequentCols();
			Map<String, String> antsToCols = new HashMap<>();

			for (Record r : mRecs) {
				antsToCols.put(r.getRecordStr(ants), r.getRecordStr(cons));
			}

			consToAntsCols.put(constraint, antsToCols);
		}

		for (int i = 0; i < numIncorrect; i++) {
			ErrorMetadata emd = bstEmds.get(i);
			Record tgtRec = tgtDataset.getRecord(emd.getTid());
			Record cTgtRec = null;
			boolean canAdd = false;
			int numAppend = 0;
			while (!canAdd) {
				numAppend++;
				cTgtRec = copyRecord(tgtRec);
				Map<String, String> cToV = cTgtRec.getColsToVal();

				for (int j = 0; j < unionAntecendents.size(); j++) {
					String ant = unionAntecendents.get(j);
					// Should not make the mrecs inconsistent.
					String randStr = RandomStringUtils.random(numAppend, 97,
							125, true, false, null, rand);
					String val = cToV.get(ant) + randStr;
					cTgtRec.modifyValForExistingCol(ant, val);
					// logger.log(ProdLevel.PROD, "\nAdding incorrect match: " +
					// ant
					// + ", " + val);
				}

				int numConsSatisfied = 0;
				for (Map.Entry<Constraint, Map<String, String>> entry : consToAntsCols
						.entrySet()) {
					Constraint c = entry.getKey();
					Map<String, String> antsToCons = entry.getValue();

					List<String> antCols = c.getAntecedentCols();
					List<String> consCols = c.getConsequentCols();
					String newAnt = cTgtRec.getRecordStr(antCols);
					String newCons = cTgtRec.getRecordStr(consCols);

					if (antsToCons.containsKey(newAnt)) {
						if (newCons.equals(antsToCons.get(newAnt))) {
							numConsSatisfied++;
						}
					} else {
						numConsSatisfied++;
					}
				}

				if (numConsSatisfied == consToAntsCols.keySet().size()) {
					for (Map.Entry<Constraint, Map<String, String>> entry : consToAntsCols
							.entrySet()) {
						Constraint c = entry.getKey();
						Map<String, String> antsToCons = entry.getValue();

						List<String> antCols = c.getAntecedentCols();
						List<String> consCols = c.getConsequentCols();
						String newAnt = cTgtRec.getRecordStr(antCols);
						String newCons = cTgtRec.getRecordStr(consCols);

						antsToCons.put(newAnt, newCons);
					}

					canAdd = true;
				}
			}

			mRecs.add(cTgtRec);
		}
	}

	private List<ErrorMetadata> getEmdsBelowSimThreshold(
			List<ErrorMetadata> emds, TargetDataset tgtDataset,
			float simThreshold) {
		List<Violations> orderedViols = repairService
				.orderViolations(tgtDataset);
		Map<Constraint, Set<Long>> consToRecs = new HashMap<>();

		for (Violations v : orderedViols) {
			Collection<Record> recs = v.getViolMap().values();
			Set<Long> rids = new HashSet<>();

			for (Record r : recs) {
				rids.add(r.getId());
			}

			consToRecs.put(v.getConstraint(), rids);
		}

		List<ErrorMetadata> bstEmds = new ArrayList<>();
		Map<Constraint, Set<String>> consToCols = new HashMap<>();
		for (Constraint c : consToRecs.keySet()) {
			consToCols.put(c, new HashSet<>(c.getColsInConstraint()));
		}

		for (ErrorMetadata emd : emds) {
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
								tgtDataset.getRecord(emd.getTid()), c);
						if (s < simThreshold) {
							bstEmds.add(emd);
						}

						break;
					}
				}
			}
		}

		return bstEmds;
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

	private double calcSim(int levDist, int s1Len, int s2Len) {
		double simForCons = 1.0f - (float) levDist
				/ (float) Math.max(s1Len, s2Len);
		return simForCons;
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
	public void removeInfoContentUsingCandidate(InfoContentTable table,
			MasterDataset mDataset, List<Recommendation> recs) {
		double[][] data = table.getData();
		BiMap<String, Integer> colNameToId = table.getColIdToName().inverse();

		if (recs == null || recs.isEmpty())
			return;

		if (recs == null || recs.isEmpty())
			return;
		for (Recommendation rec : recs) {
			long mRid = rec.getmRid();
			String col = rec.getCol();
			int colId = colNameToId.get(col);
			data[(int) mRid - 1][colId] = 0;

			// Keep track of what was revealed. Used in the future when building
			// info content table wrt other fds.
			mDataset.addColToRevealedId(col, mRid);
		}

		if (data.length < 100)
			logger.log(ProdLevel.PROD, "\nNew info content table : " + table);
	}

	private void removeInfoContentUsingHistory(InfoContentTable table,
			MasterDataset mDataset) {
		double[][] data = table.getData();
		BiMap<String, Integer> colNameToId = table.getColIdToName().inverse();

		Multimap<String, Long> colToRevealedId = mDataset.getColToRevealedId();

		for (String col : colToRevealedId.keySet()) {
			Collection<Long> rids = colToRevealedId.get(col);

			if (colNameToId.containsKey(col)) {
				int colId = colNameToId.get(col);

				for (long mRid : rids) {
					data[(int) mRid - 1][colId] = 0;
				}
			}

		}

	}

	@Override
	public void saveMatchesJSON(List<Match> matches, String fileName) {
		Writer writer = null;

		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName), "utf-8"));
			writer.write(gson.toJson(matches));
		} catch (IOException ex) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + ex.getMessage());
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
			}
		}
	}

	@Override
	public List<Match> loadMatchesJSON(String fileName) {
		try (FileInputStream inputStream = new FileInputStream(fileName)) {
			String matches = IOUtils.toString(inputStream);
			Type listOfTestObject = new TypeToken<ArrayList<Match>>() {
			}.getType();
			return gson.fromJson(matches, listOfTestObject);
		} catch (FileNotFoundException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}
		return null;
	}

	@Override
	public int getTargetDatasetSize(String tgtUrl) {
		LineNumberReader lnr = null;

		try {

			lnr = new LineNumberReader(new FileReader(tgtUrl));
			lnr.skip(Long.MAX_VALUE);
			// Assumes that the 1st line consists of col names.
			int size = lnr.getLineNumber();

			return size;

		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		} finally {
			try {
				lnr.close();
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}
		}

		return -1;

	}

}
