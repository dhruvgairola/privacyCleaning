package data.cleaning.core.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.RAMDirectory;

import data.cleaning.core.service.dataset.impl.Record;

public class LuceneIndex {

	private static final Logger logger = Logger.getLogger(LuceneIndex.class);
	private IndexSearcher searcher;
	private int numRecsIndexed;
	private Random rand;

	public LuceneIndex() {
		this.rand = new Random(Config.SEED);
	}

	/**
	 * @param records
	 * @param cols
	 * @param howToIndex
	 *            - Field.Index.ANALYZED is for text strings and for similarity
	 *            queries. Field.Index.NOT_ANALYZED is for strings which require
	 *            exact matches in the queries.
	 */
	public void buildIndex(List<Record> records, List<String> cols,
			@SuppressWarnings("deprecation") Field.Index howToIndex) {

		logger.log(DebugLevel.DEBUG, "Building lucene index.");

		RAMDirectory idx = new RAMDirectory();

		try {
			// Make an writer to create the index
			IndexWriter writer = new IndexWriter(idx, new IndexWriterConfig(
					org.apache.lucene.util.Version.LUCENE_48,
					new StandardAnalyzer(
							org.apache.lucene.util.Version.LUCENE_48)));

			for (Record record : records) {
				writer.addDocument(createDocument(record, cols, howToIndex));
			}

			writer.close();

			DirectoryReader r = DirectoryReader.open(idx);
			// Build an IndexSearcher using the in-memory index
			searcher = new IndexSearcher(r);

			// SpellChecker spellchecker = new SpellChecker(idx);
			// // To index a field of a user index:
			// spellchecker.indexDictionary(new LuceneDictionary(r,
			// "First Name"),
			// new IndexWriterConfig(Version.LUCENE_48,
			// new StandardAnalyzer(Version.LUCENE_48)), true);
			//
			// // String[] suggestions = spellchecker.suggestSimilar("bian", 5,
			// r, "First Name", SuggestMode.SUGGEST_ALWAYS);
			// String[] suggestions = spellchecker.suggestSimilar("bia*", 5,
			// 0.20f);
			// System.out.println("suggg "+Arrays.toString(suggestions));

			numRecsIndexed = records.size();

			logger.log(DebugLevel.DEBUG, "Built lucene index.");

		} catch (IOException ioe) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + ioe.getMessage());

		}

	}

	@SuppressWarnings("deprecation")
	private static Document createDocument(Record record, List<String> cols,
			Field.Index howToIndex) {
		Document doc = new Document();
		Map<String, String> colsToVal = record.getColsToVal();

		// Unindexed
		doc.add(new Field("rid", record.getId() + "", Field.Store.YES,
				Field.Index.NO));

		for (Map.Entry<String, String> entry : colsToVal.entrySet()) {
			String key = entry.getKey();
			String val = entry.getValue();
			if (cols.contains(key)) {
				// http://stackoverflow.com/questions/14745973/search-on-non-analyzed-field-using-lucene
				doc.add(new Field(key, val, Field.Store.YES, howToIndex));
			}
		}

		// From lucene docs : The NOT operator cannot be used with just one
		// term. Fix for this is to add some dummy field.
		doc.add(new Field("dummy", "foo", Field.Store.YES,
				Field.Index.NOT_ANALYZED));

		return doc;
	}

	public Map<Long, Double> search(Query q, int topK) {
		if (q == null)
			return null;

		Map<Long, Double> topKRecordToDist = new LinkedHashMap<>();

		try {
			// Search for the query
			TopScoreDocCollector collector = TopScoreDocCollector.create(
					numRecsIndexed, true);
			searcher.search(q, collector);

			ScoreDoc[] hits = collector.topDocs().scoreDocs;

			int hitCount = collector.getTotalHits();
			// logger.log(DebugLevel.DEBUG, hitCount +
			// " total matching documents");

			// Examine the Hits object to see if there were any matches

			if (hitCount == 0) {
				// logger.log(DebugLevel.DEBUG, "No matches were found");
			} else {

				// Iterate over the Documents in the Hits object
				for (int i = 0; i < Math.min(topK, hitCount); i++) {

					ScoreDoc scoreDoc = hits[i];
					int docId = scoreDoc.doc;
					double docScore = scoreDoc.score;

					Document doc = searcher.doc(docId);
					topKRecordToDist.put(Long.parseLong(doc.get("rid")),
							docScore);
				}
			}
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

		return topKRecordToDist;
	}

	public List<String> searchCol(Query q, String col, int numHits) {
		if (q == null)
			return null;

		List<String> colVals = new ArrayList<>();

		try {
			int resultSize = numHits == -1 ? numRecsIndexed : numHits;
			// Search for the query
			TopScoreDocCollector collector = TopScoreDocCollector.create(
					resultSize, true);
			searcher.search(q, collector);

			ScoreDoc[] hits = collector.topDocs().scoreDocs;

			int hitCount = collector.getTotalHits();

			// logger.log(DebugLevel.DEBUG, hitCount +
			// " total matching documents");

			// Examine the Hits object to see if there were any matches

			if (hitCount == 0) {
				// logger.log(DebugLevel.DEBUG, "No matches were found");
			} else {

				// Iterate over the Documents in the Hits object
				for (int i = 0; i < Math.min(resultSize, hitCount); i++) {

					ScoreDoc scoreDoc = hits[i];
					int docId = scoreDoc.doc;
					double docScore = scoreDoc.score;

					Document doc = searcher.doc(docId);
					colVals.add(doc.get(col));
				}
			}
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

		return colVals;
	}

	public Map<String, String> searchColAndCols(Query q, String col,
			List<String> cols, int numHits) {
		if (q == null)
			return null;

		Map<String, String> colToCols = new LinkedHashMap<>();

		try {
			int resultSize = numHits == -1 ? numRecsIndexed : numHits;
			// Search for the query
			TopScoreDocCollector collector = TopScoreDocCollector.create(
					resultSize, true);
			searcher.search(q, collector);

			ScoreDoc[] hits = collector.topDocs().scoreDocs;

			int hitCount = collector.getTotalHits();

			// logger.log(DebugLevel.DEBUG, hitCount +
			// " total matching documents");

			// Examine the Hits object to see if there were any matches

			if (hitCount == 0) {
				// logger.log(DebugLevel.DEBUG, "No matches were found");
			} else {

				// Iterate over the Documents in the Hits object
				for (int i = 0; i < Math.min(resultSize, hitCount); i++) {

					ScoreDoc scoreDoc = hits[i];
					int docId = scoreDoc.doc;
					double docScore = scoreDoc.score;

					Document doc = searcher.doc(docId);
					StringBuilder sb = new StringBuilder();
					for (String c : cols) {
						sb.append(doc.get(c) + " ");
					}

					colToCols.put(doc.get(col), sb.toString());
				}
			}
		} catch (IOException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

		return colToCols;
	}

	/**
	 * Returns a random subset (of size n) of the domain vals in col.
	 * 
	 * @param col
	 * @param numHits
	 * @return
	 */
	public List<String> searchRand(String col, int n) {
		List<String> toRet = new ArrayList<>();

		for (int i = 0; i < n; i++) {
			int randDocIdx = rand.nextInt(searcher.getIndexReader().maxDoc());
			Document randDoc = null;
			try {
				randDoc = searcher.doc(randDocIdx);
			} catch (IOException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}

			if (randDoc != null) {
				toRet.add(randDoc.get(col));
			}

		}

		return toRet;
	}
}
