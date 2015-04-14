package data.cleaning.core.service.matching.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Floats;

import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.matching.MatchingService;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.DebugLevel;
import data.cleaning.core.utils.DistanceMeasures;
import data.cleaning.core.utils.FileCache;
import data.cleaning.core.utils.LuceneIndex;
import data.cleaning.core.utils.Pair;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.Version;
import data.cleaning.core.utils.kdtree.EuclideanDistance;
import data.cleaning.core.utils.kdtree.KDNodeInfo;
import data.cleaning.core.utils.kdtree.KDTree;
import data.cleaning.core.utils.kdtree.KeyDuplicateException;
import data.cleaning.core.utils.kdtree.KeySizeException;

@Service("matchingService")
public class MatchingServiceImpl implements MatchingService {

	private static final Logger logger = Logger
			.getLogger(MatchingServiceImpl.class);

	@Autowired
	@Qualifier(value = "datasetService")
	protected DatasetService datasetService;
	private Random rand;

	public MatchingServiceImpl() {
		this.rand = new Random(Config.SEED);
	}

	public EmbVector dist(String value, String meta,
			List<List<String>> refSets, List<List<EmbVector>> embRefSets,
			boolean shouldApproxDist) {
		EmbVector dA = new EmbVector();
		float[] fArr = new float[refSets.size()];
		if (!shouldApproxDist) {
			for (int i = 0; i < refSets.size(); i++) {
				float dist = dist(value, refSets.get(i));
				fArr[i] = dist;
			}
		} else {
			for (int i = 0; i < refSets.size(); i++) {

				if (i == 0) {
					float dist = dist(value, refSets.get(0));
					fArr[i] = dist;

				} else {
					float coord = fArr[i - 1];
					List<EmbVector> refVects = embRefSets.get(i);
					List<String> refVectStrs = refSets.get(i);
					double lowestEucDist = Double.MAX_VALUE;
					String ref = "";

					for (int j = 0; j < refVects.size(); j++) {
						EmbVector refVect = refVects.get(j);
						double coord2 = refVect.getVector()[i - 1];
						double eucDist = Math.abs(coord2 - coord);

						if (eucDist <= lowestEucDist) {
							lowestEucDist = eucDist;
							ref = refVectStrs.get(j);
						}

					}
					fArr[i] = DistanceMeasures.getLevDistance(
							value.toLowerCase(), ref.toLowerCase());
				}
			}
		}

		dA.setVector(fArr);
		// TODO: Remove lowercase?
		dA.setMeta(meta);
		// Important because value is needed for getStress
		// calculation.
		dA.setValue(value);

		return dA;
	}

	public Pair<List<EmbVector>, List<Integer>> greedyResamplingCol(
			List<EmbVector> vectors, int reducedDim) {
		Pair<List<EmbVector>, List<Integer>> resampledP = new Pair<>();

		// logger.log(SecurityLevel.SECURITY, "Input before greedy : " +
		// vectors);

		List<EmbVector> resampled = new ArrayList<>();
		List<Integer> selectedCoords = new ArrayList<>();

		if (vectors.size() == 1) {
			EmbVector oldEmb = vectors.get(0);
			float[] oldArr = oldEmb.getVector();
			EmbVector newEmb = new EmbVector();
			float[] newArr = new float[reducedDim];
			for (int i = 0; i < reducedDim; i++) {
				newArr[i] = oldArr[i];
			}

			newEmb.setVector(newArr);
			newEmb.setMeta(oldEmb.getMeta());
			newEmb.setValue("obfuscated");

			resampled.add(newEmb);
		} else {
			List<Pair<EmbVector, EmbVector>> samples = samplePairs(vectors);
			int embVSize = vectors.get(0).getVector().length;

			for (int i = 0; i < reducedDim; i++) {
				double minStress = Double.MAX_VALUE;
				int candidateCol = 0;

				for (int j = 0; j < embVSize; j++) {

					if (!selectedCoords.contains(j)
							&& getStress(samples, selectedCoords, j) < minStress) {
						candidateCol = j;
					}
				}

				selectedCoords.add(candidateCol);
			}

			for (EmbVector ev : vectors) {
				float[] evArr = ev.getVector();
				EmbVector newEmb = new EmbVector();
				float[] newArr = new float[selectedCoords.size()];

				for (int i = 0; i < selectedCoords.size(); i++) {
					int selCoord = selectedCoords.get(i);
					newArr[i] = evArr[selCoord];
				}

				newEmb.setVector(newArr);
				newEmb.setMeta(ev.getMeta());
				newEmb.setValue("obfuscated");

				resampled.add(newEmb);
			}
		}

		resampledP.setO1(resampled);
		resampledP.setO2(selectedCoords);

		// logger.log(SecurityLevel.SECURITY, "Output after greedy : " +
		// resampled);
		return resampledP;
	}

	private double getStress(List<Pair<EmbVector, EmbVector>> samples,
			List<Integer> selectedCols, int candidateCol) {
		double nums = 0f;
		double denoms = 0f;

		for (Pair<EmbVector, EmbVector> sample : samples) {
			EmbVector v1 = sample.getO1();
			float[] v1Arr = v1.getVector();
			EmbVector v2 = sample.getO2();
			float[] v2Arr = v2.getVector();

			double d2 = (double) DistanceMeasures.getLevDistance(v1.getValue(),
					v2.getValue());

			double tot = 0d;

			for (int selCol : selectedCols) {
				tot += Math.pow(v1Arr[selCol] - v2Arr[selCol], 2);

			}
			tot += Math.pow(v1Arr[candidateCol] - v2Arr[candidateCol], 2);

			double d1 = Math.sqrt(tot);

			nums += Math.pow(d1 - d2, 2);
			denoms += Math.pow(d1, 2);
		}

		return Math.sqrt(nums / denoms);
	}

	/**
	 * Roughly sample half the dataset.
	 * 
	 * @param vs
	 * @return
	 */
	private List<Pair<EmbVector, EmbVector>> samplePairs(List<EmbVector> vs) {
		// TODO: Improve this
		Set<Pair<Integer, Integer>> pairs = new HashSet<>();

		while (pairs.size() < vs.size() / 2) {
			int r1 = rand.nextInt(vs.size());
			int r2 = rand.nextInt(vs.size());

			while (r1 == r2) {
				r1 = rand.nextInt(vs.size());
				r2 = rand.nextInt(vs.size());
			}

			Pair<Integer, Integer> p = new Pair<>();
			p.setO1(r1);
			p.setO2(r2);

			Pair<Integer, Integer> pRev = new Pair<>();
			p.setO1(r2);
			p.setO2(r1);

			if (!pairs.contains(pRev)) {
				pairs.add(p);
			}
		}

		List<Pair<EmbVector, EmbVector>> samples = new ArrayList<>();

		for (Pair<Integer, Integer> pair : pairs) {
			Pair<EmbVector, EmbVector> e = new Pair<>();
			e.setO1(vs.get(pair.getO1()));
			e.setO2(vs.get(pair.getO2()));

			samples.add(e);
		}

		return samples;
	}

	public float dist(String value, List<String> refSet) {
		float minEditDist = Float.MAX_VALUE;

		for (String ref : refSet) {
			// TODO: remove toLowerCase?
			float ed = DistanceMeasures.getLevDistance(value.toLowerCase(),
					ref.toLowerCase());
			if (ed < minEditDist) {
				minEditDist = ed;
			}
		}

		return minEditDist;
	}

	public List<List<EmbVector>> embedRefSets(List<List<String>> refSets) {
		List<List<EmbVector>> emRefSets = new ArrayList<>();

		for (List<String> refSet : refSets) {
			List<EmbVector> vectors = new ArrayList<>();

			for (String ref : refSet) {
				EmbVector v = new EmbVector();
				float[] vArr = new float[refSets.size()];

				for (int i = 0; i < refSets.size(); i++) {
					List<String> refSet2 = refSets.get(i);
					vArr[i] = dist(ref, refSet2);
				}

				v.setVector(vArr);
				v.setMeta(ref);
				v.setValue("Ref set.");
				vectors.add(v);

			}

			emRefSets.add(vectors);
		}

		return emRefSets;
	}

	public List<List<String>> getRefSets(List<String> generator) {
		List<List<String>> refSets = new ArrayList<>();
		int genSize = generator.size();

		int numRefSets = (int) Math.pow(
				Math.floor(Math.log((double) genSize) / Math.log(2f)), 2);

		for (int i = 0; i < numRefSets; i++) {
			List<String> refSet = new ArrayList<>();

			int q = (int) Math.floor(((double) ((i + 1) - 1) / (Math
					.log((double) genSize) / Math.log(2f))) + 1f);

			int sizeOfRefSet = (int) Math.pow(2, q);

			for (int j = 0; j < sizeOfRefSet; j++) {

				int chosenStrIndex = rand.nextInt(genSize);
				String add = generator.get(chosenStrIndex);

				refSet.add(add);
			}

			refSets.add(refSet);
		}

		return refSets;
	}

	/*
	 * Must be similar alphabet with the data to be embedded. (non-Javadoc)
	 * 
	 * @see
	 * data.cleaning.core.service.privacy.PrivacyService#getRandomStrings(int,
	 * int, boolean)
	 */
	public List<String> getRandomStrings(int strLen, int numStrs,
			boolean isNumeric) {
		List<String> strs = new ArrayList<>();

		for (int i = 0; i < numStrs; i++) {
			if (isNumeric) {
				String numStr = RandomStringUtils.random(strLen, 0, 0, false,
						true, null, rand);
				strs.add(numStr);
			} else {
				String alphStr = RandomStringUtils.random(strLen, 0, 0, true,
						false, null, rand);
				strs.add(alphStr);
			}
		}

		return strs;
	}

	@Override
	public List<Match> applyPvtApproxDataMatching(Constraint constraint,
			List<Record> tgtRecords, List<Record> mRecords, int numStrs,
			double dimReduction, float simThreshold, boolean shouldApproxDist,
			boolean shouldAvg, String tgtFileName, String mFileName) {

		if (tgtRecords == null || tgtRecords.isEmpty() || mRecords == null
				|| mRecords.isEmpty())
			return null;
		List<String> cols = constraint.getColsInConstraint();
		List<Match> cachedMatches = shouldUseFileCache(constraint, tgtFileName,
				mFileName, simThreshold);
		if (cachedMatches != null && !cachedMatches.isEmpty()) {
			logger.log(ProdLevel.PROD, "Loading matches from file cache.");
			return cachedMatches;
		}

		logger.log(ProdLevel.PROD, "Calc. matches.");

		// logger.log(DebugLevel.DEBUG, "\nAll Matches : \n");
		// logger.log(DebugLevel.DEBUG,
		// "\n---------Matching Explanation---------\n\n");

		Map<String, String> sampleRecord = tgtRecords.get(0).getColsToVal();

		Map<String, Boolean> colToType = new LinkedHashMap<>();

		int strLen = estimateStrLen(mRecords, cols);

		// logger.log(DebugLevel.DEBUG, "Str length is : " + strLen);

		List<String> numGenerators = getRandomStrings(strLen, numStrs, true);
		List<List<String>> numRefSets = getRefSets(numGenerators);

		List<List<EmbVector>> numEmbRefSets = embedRefSets(numRefSets);

		List<String> alphaGenerators = getRandomStrings(strLen, numStrs, false);
		List<List<String>> alphaRefSets = getRefSets(alphaGenerators);
		List<List<EmbVector>> alphaEmbRefSets = embedRefSets(alphaRefSets);

		for (Map.Entry<String, String> entry : sampleRecord.entrySet()) {
			colToType.put(entry.getKey(),
					StringUtils.isNumeric(entry.getValue()));
		}

		// logger.log(DebugLevel.DEBUG, "Numeric Generators : " +
		// numGenerators);
		// logger.log(DebugLevel.DEBUG, "Alpha Generators : " +
		// alphaGenerators);
		// logger.log(DebugLevel.DEBUG, "Numeric Ref Sets : " + numRefSets);
		// logger.log(DebugLevel.DEBUG, "Embedded Numeric Ref Sets : "
		// + numEmbRefSets + "\n");
		// logger.log(DebugLevel.DEBUG, "Alpha Ref Sets : " + alphaRefSets);
		// logger.log(DebugLevel.DEBUG, "Embedded Alpha Ref Sets : "
		// + alphaEmbRefSets + "\n");

		EmbPrivateDataset embTgt = embedDataset(numRefSets, numEmbRefSets,
				alphaRefSets, alphaEmbRefSets, cols, tgtRecords, false,
				colToType, shouldApproxDist);
		EmbPrivateDataset embMaster = embedDataset(numRefSets, numEmbRefSets,
				alphaRefSets, alphaEmbRefSets, cols, mRecords, true, colToType,
				shouldApproxDist);

		int reducedDim = (int) (dimReduction * embTgt.getVectTable()[0][0]
				.getVector().length);

		// logger.log(DebugLevel.DEBUG, "Embedded target viols : " + embTgt);
		// logger.log(DebugLevel.DEBUG, "Embedded master : " + embMaster);

		if (dimReduction != 1.0d) {
			// TODO: Changing greedy resampling will really improve accuracy a
			// lot!

			// We cannot perform greedy resampling together for both sets
			// because getStress requires the attribute value.
			// But we can perform an optimization- Since we know master is
			// clean, we
			// can get the most discriminating coords using it. Otherwise, we'll
			// likely get a mismatch. But does this extra info preserve the
			// security
			// properties? Note that efficiency (along with quality) will
			// improve
			// too since we don't need to do greedy resampling procedure for the
			// target.
			greedyResampling(embMaster, reducedDim);

			greedyResampling(embTgt, reducedDim);

		}

		// Normalization, perform together for both sets.
		normalize(embTgt, embMaster);

		List<Match> tgtMatches = null;

		if (shouldAvg) {
			KDTree<List<KDNodeInfo>> indexMaster = buildIndex(embMaster,
					reducedDim);

			tgtMatches = getPvtMatches(embTgt, embMaster, indexMaster,
					simThreshold);
		} else {
			List<KDTree<List<KDNodeInfo>>> indicesMaster = buildIndices(
					embMaster, reducedDim);

			tgtMatches = getPvtMatches(embTgt, embMaster, indicesMaster,
					simThreshold);
		}

		// logger.log(DebugLevel.DEBUG,
		// "\n---------End explanation---------\n\n");

		saveToFileCache(constraint, tgtMatches, tgtFileName, mFileName,
				simThreshold);

		return tgtMatches;
	}

	private List<Match> shouldUseFileCache(Constraint constraint,
			String tgtFileName, String mFileName, float simThreshold) {
		String key = genMatchSignature(constraint, tgtFileName, mFileName,
				simThreshold);

		if (Config.FILE_CACHE.contains(FileCache.USE_MATCHES_CACHE)) {
			return datasetService.loadMatchesJSON(Config.FILE_CACHE_BASE_URL
					+ key + ".txt");
		}

		return null;
	}

	private String genMatchSignature(Constraint constraint, String tgtFileName,
			String mFileName, float simThreshold) {
		// TODO : Find a better way to do this.
		String fileName = "tgt" + tgtFileName + "_m" + mFileName + "_cid"
				+ constraint.getId() + "_topk" + Config.TOP_K_MATCHES + "_sa"
				+ (Config.SHOULD_AVERAGE ? 1 : 0) + "_dr"
				+ Config.DIM_REDUCTION + "_numstr" + Config.NUM_STRINGS
				+ "_opt" + (Config.SHOULD_APPROX_DIST ? 1 : 0) + "_sim"
				+ simThreshold;
		return fileName;
	}

	private void saveToFileCache(Constraint constraint, List<Match> tgtMatches,
			String tgtFileName, String mFileName, float simThreshold) {
		String key = genMatchSignature(constraint, tgtFileName, mFileName,
				simThreshold);
		datasetService.saveMatchesJSON(tgtMatches, Config.FILE_CACHE_BASE_URL
				+ key + ".txt");
	}

	/**
	 * Combining all the columns into 1 index. Faster and saves memory. But does
	 * not use decision rule matching as prescribed by Scannapieco et Al 2007.
	 * 
	 * @param d
	 * @param reducedDim
	 * @return
	 */
	private KDTree<List<KDNodeInfo>> buildIndex(EmbPrivateDataset d,
			int reducedDim) {
		logger.log(DebugLevel.DEBUG, "Building single index.");
		EmbVector[][] vectTable = d.getVectTable();
		KDTree<List<KDNodeInfo>> idx = new KDTree<List<KDNodeInfo>>(reducedDim
				* vectTable[0].length);
		Map<EmbVector, List<KDNodeInfo>> vectToNodeInfos = new HashMap<>();

		for (int row = 0; row < vectTable.length; row++) {
			EmbVector combined = new EmbVector();
			List<EmbVector> chunked = new ArrayList<>();
			int oneVectSize = vectTable[0][0].getVector().length;
			float[][] combinedVArr = new float[vectTable[0].length][oneVectSize];

			for (int col = 0; col < vectTable[0].length; col++) {
				float[] vArr = vectTable[row][col].getVector();
				EmbVector c = new EmbVector();
				c.setVector(vArr);
				chunked.add(c);

				combinedVArr[col] = vArr;
			}

			float[] fCombinedVArr = Floats.concat(combinedVArr);
			combined.setVector(fCombinedVArr);
			EmbVector combinedVKey = new EmbVector();
			combinedVKey.setVector(fCombinedVArr);

			KDNodeInfo info = new KDNodeInfo();
			info.setrId(d.getRId(row));
			info.setRowId(row);
			info.setEmbVect(combined);
			info.setChunkedEmbVector(chunked);

			try {
				if (vectToNodeInfos.containsKey(combinedVKey)) {
					// Handle duplicate key.
					List<KDNodeInfo> nodeInfos = vectToNodeInfos
							.get(combinedVKey);
					nodeInfos.add(info);
				} else {
					List<KDNodeInfo> nodeInfos = new ArrayList<>();
					nodeInfos.add(info);
					vectToNodeInfos.put(combinedVKey, nodeInfos);
					idx.insert(fCombinedVArr, nodeInfos);
				}

			} catch (KeySizeException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			} catch (KeyDuplicateException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}

		}

		idx.setDistanceMetric(new EuclideanDistance());

		logger.log(DebugLevel.DEBUG, "Built single index.");
		return idx;
	}

	private int estimateStrLen(List<Record> records, List<String> cols) {
		int numSamples = 0;
		int num = 0;
		int denom = 0;

		for (int i = 0; i < records.size(); i++) {

			// Sample about 1/3rd of the records, but not too many.
			if (i % 3 == 0 && numSamples < 50) {
				Record s = records.get(i);
				Map<String, String> colsToVal = s.getColsToVal();

				for (Map.Entry<String, String> entry : colsToVal.entrySet()) {
					if (cols != null && !cols.contains(entry.getKey())) {
						continue;
					} else {
						num += entry.getValue().length();
						denom++;
					}
				}

				numSamples++;
			}
		}

		return num / denom;
	}

	private void normalize(EmbPrivateDataset embTgt, EmbPrivateDataset embMaster) {
		// TODO : Find a better way to normalize?

		EmbVector[][] tgtVectTable = embTgt.getVectTable();
		EmbVector[][] masterVectTable = embMaster.getVectTable();

		for (int row = 0; row < tgtVectTable.length; row++) {
			for (int col = 0; col < tgtVectTable[0].length; col++) {
				float[] vectArr = tgtVectTable[row][col].getVector();
				float norm = 0f;
				for (float v : vectArr) {
					norm += v * v;
				}

				norm = (float) Math.sqrt(norm);

				for (int i = 0; i < vectArr.length; i++) {
					vectArr[i] = vectArr[i] / norm;
				}
			}
		}

		for (int row = 0; row < masterVectTable.length; row++) {
			for (int col = 0; col < masterVectTable[0].length; col++) {
				float[] vectArr = masterVectTable[row][col].getVector();
				float norm = 0f;
				for (float v : vectArr) {
					norm += v * v;
				}

				norm = (float) Math.sqrt(norm);

				for (int i = 0; i < vectArr.length; i++) {
					vectArr[i] = vectArr[i] / norm;
				}
			}
		}

		logger.log(DebugLevel.DEBUG,
				"Normalized embedded target (dim reduction) : " + embTgt);
		logger.log(DebugLevel.DEBUG,
				"Normalized embedded master (dim reduction) : " + embMaster);

	}

	/**
	 * Since we are using the decision rule with threshold, we normalize both
	 * the master and target as if they were 1 vector.
	 * 
	 * @param datasets
	 * @return
	 */
	private void normalizeWeird(EmbPrivateDataset embTgt,
			EmbPrivateDataset embMaster) {
		// TODO : Find a better way to normalize?

		EmbVector[][] tgtVectTable = embTgt.getVectTable();
		EmbVector[][] masterVectTable = embMaster.getVectTable();

		for (int col = 0; col < tgtVectTable[0].length; col++) {
			float[] norms = new float[tgtVectTable[0][col].getVector().length];

			for (int row = 0; row < tgtVectTable.length; row++) {

				float[] vectArr = tgtVectTable[row][col].getVector();

				for (int i = 0; i < vectArr.length; i++) {
					norms[i] += vectArr[i] * vectArr[i];
				}

			}

			for (int row = 0; row < masterVectTable.length; row++) {

				float[] vectArr = masterVectTable[row][col].getVector();

				for (int i = 0; i < vectArr.length; i++) {
					norms[i] += vectArr[i] * vectArr[i];
				}
			}

			for (int i = 0; i < norms.length; i++) {
				norms[i] = (float) Math.sqrt(norms[i]);
			}

			for (int row = 0; row < tgtVectTable.length; row++) {
				EmbVector v = tgtVectTable[row][col];
				float[] vectArr = v.getVector();

				for (int coord = 0; coord < vectArr.length; coord++) {
					vectArr[coord] = vectArr[coord] / norms[coord];
				}
			}

			for (int row = 0; row < masterVectTable.length; row++) {
				EmbVector v = masterVectTable[row][col];
				float[] vectArr = v.getVector();

				for (int coord = 0; coord < vectArr.length; coord++) {
					vectArr[coord] = vectArr[coord] / norms[coord];
				}
			}
		}

		// logger.log(DebugLevel.DEBUG,
		// "Normalized embedded target (dim reduction) : " + embTgt);
		// logger.log(DebugLevel.DEBUG,
		// "Normalized embedded master (dim reduction) : " + embMaster);

	}

	public List<KDTree<List<KDNodeInfo>>> buildIndices(EmbPrivateDataset d,
			int reducedDim) {
		List<KDTree<List<KDNodeInfo>>> idxs = new ArrayList<>();
		EmbVector[][] vectTable = d.getVectTable();
		// Map<Integer, Long> rowIdToRId = d.getRowIdToRId();

		for (int col = 0; col < vectTable[0].length; col++) {
			KDTree<List<KDNodeInfo>> idx = new KDTree<List<KDNodeInfo>>(
					reducedDim);

			Multimap<EmbVector, KDNodeInfo> vectToNodeInfos = LinkedHashMultimap
					.create();

			for (int row = 0; row < vectTable.length; row++) {
				KDNodeInfo info = new KDNodeInfo();
				info.setrId(d.getRId(row));
				info.setRowId(row);
				info.setEmbVect(vectTable[row][col]);
				// All records with the same column are gathered in the same
				// bucket.
				vectToNodeInfos.put(vectTable[row][col], info);
			}

			for (EmbVector v : vectToNodeInfos.keySet()) {
				try {
					idx.insert(v.getVector(), new ArrayList<KDNodeInfo>(
							vectToNodeInfos.get(v)));

					// logger.log(SecurityLevel.SECURITY, "Inserting :  "
					// + Arrays.toString(v.getDoubleArray()));

				} catch (KeySizeException e) {
					logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
				} catch (KeyDuplicateException e) {
					logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
				}
			}

			idx.setDistanceMetric(new EuclideanDistance());

			idxs.add(idx);
		}

		logger.log(DebugLevel.DEBUG, "Built indexes.");
		return idxs;
	}

	private List<Match> getPvtMatches(EmbPrivateDataset loopingOver,
			EmbPrivateDataset indexed, KDTree<List<KDNodeInfo>> idx,
			double simThreshold) {
		List<Match> matches = new ArrayList<>();
		String[] cols = loopingOver.getCols();
		EmbVector[][] loopingOverTable = loopingOver.getVectTable();
		boolean shouldRemoveDupMatches = Config.VERSION
				.contains(Version.REMOVE_DUPS_FROM_MATCHES);
		for (int row = 0; row < loopingOverTable.length; row++) {
			if (row % 1000 == 0)
				logger.log(DebugLevel.DEBUG, "calc matches (row) : " + row);
			float[][] combinedVArr = new float[loopingOverTable[0].length][loopingOverTable[0][0]
					.getVector().length];
			List<float[]> chunkedTgt = new ArrayList<>();

			for (int col = 0; col < loopingOverTable[0].length; col++) {
				float[] tv = loopingOverTable[row][col].getVector();
				chunkedTgt.add(tv);
				combinedVArr[col] = tv;
			}

			float[] fCombinedVArr = Floats.concat(combinedVArr);

			try {
				List<List<KDNodeInfo>> mss = idx.nearestDistance(fCombinedVArr,
						1f - (float) simThreshold);

				// logger.log(DebugLevel.DEBUG, "calc matches (neighbs done)");

				Match m = new Match();
				m.setMatchId(row);
				m.setOriginalrId(loopingOver.getRId(row));

				for (List<KDNodeInfo> ms : mss) {
					for (KDNodeInfo m2 : ms) {
						// Drill down to get dist. between column values.
						if (Config.VERSION
								.contains(Version.REMOVE_EXACT_MATCHES)) {
							List<EmbVector> chunkedMaster = m2
									.getChunkedEmbVector();
							float totDist = 0f;
							for (int col = 0; col < chunkedMaster.size(); col++) {
								EmbVector chunkMaster = chunkedMaster.get(col);
								float[] chunkTgt = chunkedTgt.get(col);
								float distC = idx.getDistanceMetric().distance(
										chunkTgt, chunkMaster.getVector());

								if (distC > Config.FLOAT_EQUALIY_EPSILON) {
									m.addRidAndNonExactCol(m2.getrId(),
											cols[col]);
								}
								totDist += distC;
							}

							float dist = totDist / (float) chunkedMaster.size();
							// Don't add exact matches.
							if (dist > Config.FLOAT_EQUALIY_EPSILON)
								m.addRidAndDistIfAcceptable(m2.getrId(), dist,
										shouldRemoveDupMatches);

						} else if (Config.VERSION
								.contains(Version.BASIC_REMOVE_EXACT_MATCHES)) {

							float dist = idx.getDistanceMetric().distance(
									fCombinedVArr,
									m2.getEmbVector().getVector());

							// Don't add exact matches.
							if (dist > Config.FLOAT_EQUALIY_EPSILON)
								m.addRidAndDistIfAcceptable(m2.getrId(), dist,
										shouldRemoveDupMatches);
						} else {
							float dist = idx.getDistanceMetric().distance(
									fCombinedVArr,
									m2.getEmbVector().getVector());

							m.addRidAndDistIfAcceptable(m2.getrId(), dist,
									shouldRemoveDupMatches);
						}

					}
				}
				matches.add(m);

			} catch (KeySizeException e) {
				logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
			}

		}

		return matches;
	}

	private List<Match> getPvtMatches(EmbPrivateDataset loopingOver,
			EmbPrivateDataset indexed, List<KDTree<List<KDNodeInfo>>> idxs,
			float simThreshold) {
		List<Match> matches = new ArrayList<>();
		String[] cols = loopingOver.getCols();
		boolean shouldRemoveDupMatches = Config.VERSION
				.contains(Version.REMOVE_DUPS_FROM_MATCHES);

		// Map<Integer, Long> idxRowIdToRId = indexed.getRowIdToRId();
		EmbVector[][] loopingOverTable = loopingOver.getVectTable();
		EmbVector[][] indexedTable = indexed.getVectTable();

		for (int row = 0; row < loopingOverTable.length; row++) {
			logger.log(DebugLevel.DEBUG, "calc matches (row)" + row);
			int[] counts = new int[indexedTable.length];
			Map<Integer, List<Double>> matchRowIdToDists = new LinkedHashMap<>();

			for (int col = 0; col < loopingOverTable[0].length; col++) {
				KDTree<List<KDNodeInfo>> idx = idxs.get(col);

				try {

					List<List<KDNodeInfo>> mss = idx.nearestDistance(
							loopingOverTable[row][col].getVector(),
							1f - simThreshold);

					logger.log(DebugLevel.DEBUG, "calc matches (neighbs done)");

					for (List<KDNodeInfo> ms : mss) {
						for (KDNodeInfo m2 : ms) {
							// System.out
							// .println("Looping over: "
							// + Arrays.toString(loopingOverTable[row][col]
							// .getDoubleArray())
							// + " RecId : "
							// + (int) loopingOver.getRId(row)
							// + ", Comparing with: "
							// + Arrays.toString(m2.getEmbVector()
							// .getDoubleArray())
							// + ", RecId: "
							// + m2.getrId()
							// + ", Dist bet : "
							// + idx.getDistanceMetric().distance(
							// loopingOverTable[row][col]
							// .getDoubleArray(),
							// m2.getEmbVector()
							// .getDoubleArray()));
							counts[m2.getRowId()]++;

							double dists = idx.getDistanceMetric().distance(
									loopingOverTable[row][col].getVector(),
									m2.getEmbVector().getVector());

							if (matchRowIdToDists.containsKey(m2.getRowId())) {
								List<Double> coords = matchRowIdToDists.get(m2
										.getRowId());
								coords.add(dists);
							} else {
								List<Double> coords = new ArrayList<>();
								coords.add(dists);
								matchRowIdToDists.put(m2.getRowId(), coords);
							}

						}
					}

				} catch (KeySizeException e) {
					logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
				}

			}

			Match m = new Match();
			m.setOriginalrId(loopingOver.getRId(row));
			int decRuleMatchReq = loopingOverTable[0].length;

			// logger.log(SecurityLevel.SECURITY, "Original record: " +
			// m.getOriginalrId()
			// + ", Counts : " + Arrays.toString(counts));

			for (int i = 0; i < counts.length; i++) {
				if (counts[i] == decRuleMatchReq) {

					List<Double> distsL = matchRowIdToDists.get(i);
					float tot = 0f;

					for (int col = 0; col < distsL.size(); col++) {
						double dist = distsL.get(col);
						// TODO: To save memory, add only the non-exact matches.
						if (Config.VERSION
								.contains(Version.REMOVE_EXACT_MATCHES)) {
							if (dist > Config.FLOAT_EQUALIY_EPSILON)
								m.addRidAndNonExactCol(indexed.getRId(i),
										cols[col]);
						}
						tot += dist;

					}

					m.addRidAndDistIfAcceptable(indexed.getRId(i), tot
							/ decRuleMatchReq, shouldRemoveDupMatches);
				}

			}

			m.setMatchId(row);
			matches.add(m);
		}

		return matches;
	}

	private List<List<Integer>> greedyResampling(EmbPrivateDataset embDataset,
			int reducedDim) {

		List<List<Integer>> selectedCoordsForEachCol = new ArrayList<>();

		if (selectedCoordsForEachCol == null
				|| selectedCoordsForEachCol.isEmpty()) {
			// Don't apply Dhruv's column heuristic.
			logger.log(DebugLevel.DEBUG,
					"Not applying Dhruv's column heuristic.");

			selectedCoordsForEachCol = new ArrayList<>();

			EmbVector[][] vectTable = embDataset.getVectTable();

			for (int col = 0; col < vectTable[0].length; col++) {
				List<EmbVector> vs = new ArrayList<>();

				for (int row = 0; row < vectTable.length; row++) {
					vs.add(vectTable[row][col]);
				}

				Pair<List<EmbVector>, List<Integer>> vs2Pair = greedyResamplingCol(
						vs, reducedDim);
				selectedCoordsForEachCol.add(vs2Pair.getO2());

				List<EmbVector> vs2 = vs2Pair.getO1();

				for (int row = 0; row < vectTable.length; row++) {
					embDataset.addVector(embDataset.getRId(row), row, col,
							vs2.get(row));
				}

			}
		} else {
			// Apply Dhruv's column heuristic.
			logger.log(DebugLevel.DEBUG, "Applying Dhruv's column heuristic.");

			EmbVector[][] vectTable = embDataset.getVectTable();

			for (int col = 0; col < vectTable[0].length; col++) {
				List<Integer> selectedCoords = selectedCoordsForEachCol
						.get(col);

				for (int row = 0; row < vectTable.length; row++) {
					EmbVector newEmb = new EmbVector();
					float[] newEmbArr = new float[selectedCoords.size()];
					EmbVector ev = vectTable[row][col];
					float[] evArr = ev.getVector();

					for (int i = 0; i < selectedCoords.size(); i++) {
						int selCoord = selectedCoords.get(i);
						newEmbArr[i] = evArr[selCoord];
					}

					newEmb.setVector(newEmbArr);
					newEmb.setMeta(ev.getMeta());
					newEmb.setValue("obfuscated");

					embDataset.addVector(embDataset.getRId(row), row, col,
							newEmb);
				}

			}

		}

		// logger.log(DebugLevel.DEBUG, "Embedded "
		// + (embDataset.isMaster() ? "master" : "target")
		// + " (dim reduction) : " + embDataset);

		return selectedCoordsForEachCol;
	}

	private EmbPrivateDataset embedDataset(List<List<String>> numRefSets,
			List<List<EmbVector>> numEmbRefSets,
			List<List<String>> alphaRefSets,
			List<List<EmbVector>> alphaEmbRefSets, List<String> cols,
			List<Record> records, boolean isMaster,
			Map<String, Boolean> colToType, boolean shouldApproxDist) {

		EmbPrivateDataset embD = new EmbPrivateDataset(records.size(),
				cols == null ? records.get(0).getColsToVal().size()
						: cols.size());

		String[] colsArr = new String[cols.size()];

		for (int i = 0; i < records.size(); i++) {
			Record r = records.get(i);
			Map<String, String> colsToVal = r.getColsToVal();
			int attrId = 0;

			for (Map.Entry<String, String> entry : colsToVal.entrySet()) {
				if (cols != null && !cols.contains(entry.getKey())) {
					continue;
				} else {
					if (i == 0) {
						colsArr[attrId] = entry.getKey();
					}

					EmbVector eVect = dist(entry.getValue(), attrId + "",
							colToType.get(entry.getKey()) ? numRefSets
									: alphaRefSets, colToType.get(entry
									.getKey()) ? numEmbRefSets
									: alphaEmbRefSets, shouldApproxDist);
					embD.addVector(r.getId(), i, attrId, eVect);
					attrId++;
				}
			}
		}

		embD.setCols(colsArr);
		embD.setMaster(isMaster);
		return embD;
	}

	@SuppressWarnings("deprecation")
	@Override
	public List<Match> applyApproxDataMatching(Constraint constraint,
			List<Record> tgtRecords, List<Record> mRecords, float simThreshold,
			String tgtFileName, String mFileName) {
		List<String> cols = constraint.getColsInConstraint();
		List<Match> cachedMatches = shouldUseFileCache(constraint, tgtFileName,
				mFileName, simThreshold);
		boolean shouldRemoveDupMatches = Config.VERSION
				.contains(Version.REMOVE_DUPS_FROM_MATCHES);

		if (cachedMatches != null && !cachedMatches.isEmpty()) {
			logger.log(ProdLevel.PROD, "Loading matches from file cache.");
			return cachedMatches;
		}

		logger.log(ProdLevel.PROD, "Calc. matches.");

		List<Match> matches = new ArrayList<>();
		LuceneIndex idx = new LuceneIndex();
		// We want to add errors which are similar to the consequent and also
		// exist in the domain.
		idx.buildIndex(mRecords, cols, Field.Index.ANALYZED);

		for (int i = 0; i < tgtRecords.size(); i++) {

			Record tgtRec = tgtRecords.get(i);
			Map<String, String> tColsToVal = tgtRec.getColsToVal();
			String colStr = tgtRec.getRecordStr(cols);
			Map<Long, Double> mRecs = idx.search(
					buildApproxMatchingQuery(colStr, cols),
					Config.TOP_K_MATCHES);
			if (mRecs == null || mRecs.isEmpty())
				continue;

			Match m = new Match();
			if (Config.VERSION.contains(Version.REMOVE_EXACT_MATCHES)) {
				int addedMatch = 0;

				for (Map.Entry<Long, Double> e : mRecs.entrySet()) {
					int mid = e.getKey().intValue();

					Record mRec = mRecords.get(mid - 1);
					String matchStr = mRec.getRecordStr(cols);
					// The docscore is pointless as a distance so calc lev dist.
					int levDist = DistanceMeasures.getLevDistance(colStr,
							matchStr);
					float sim = calcSim(levDist, colStr.length(),
							matchStr.length());

					// Toggle rule : Have at least 1 match?
					// if (addedMatch == 0) {
					// // Add at least 1 match.
					// m.addRidAndDistIfAcceptable(e.getKey(), sim,
					// shouldRemoveDupMatches);
					//
					// Map<String, String> mColsToVal = mRec.getColsToVal();
					//
					// for (String col : cols) {
					// if (!mColsToVal.get(col)
					// .equals(tColsToVal.get(col))) {
					// m.addRidAndNonExactCol(mid, col);
					// }
					// }
					//
					// addedMatch++;
					// } else {
					if (sim >= simThreshold) {
						// logger.log(ProdLevel.PROD, "levDist : " + levDist
						// + ", sim :" + sim + " colstr :" + colStr
						// + " matchStr : " + matchStr);

						m.addRidAndDistIfAcceptable(e.getKey(), sim,
								shouldRemoveDupMatches);

						Map<String, String> mColsToVal = mRec.getColsToVal();

						for (String col : cols) {
							if (!mColsToVal.get(col)
									.equals(tColsToVal.get(col))) {
								m.addRidAndNonExactCol(mid, col);
							}
						}
					}
				}

				// }
			} else {
				int addedMatch = 0;

				for (Map.Entry<Long, Double> e : mRecs.entrySet()) {
					int mid = e.getKey().intValue();
					Record mRec = mRecords.get(mid - 1);
					String matchStr = mRec.getRecordStr(cols);

					// The docscore is pointless as a distance so calc lev dist.
					int levDist = DistanceMeasures.getLevDistance(colStr,
							matchStr);

					float sim = calcSim(levDist, colStr.length(),
							matchStr.length());

					// Toggle rule : Have at least 1 match?
					// if (addedMatch == 0) {
					// m.addRidAndDistIfAcceptable(e.getKey(), sim,
					// shouldRemoveDupMatches);
					// addedMatch++;
					// } else {
					if (sim >= simThreshold) {
						m.addRidAndDistIfAcceptable(e.getKey(), sim,
								shouldRemoveDupMatches);
					}
					// }

				}
			}

			m.setMatchId(i);
			m.setOriginalrId(tgtRec.getId());
			matches.add(m);
		}

		saveToFileCache(constraint, matches, tgtFileName, mFileName,
				simThreshold);

		return matches;
	}

	private float calcSim(int levDist, int s1Len, int s2Len) {
		float simForCons = 1.0f - (float) levDist
				/ (float) Math.max(s1Len, s2Len);
		return simForCons;
	}

	public Query buildApproxMatchingQuery(String colsStr, List<String> cols) {

		StringBuilder sb = new StringBuilder();
		sb.append(colsStr);
		sb.append("~");

		MultiFieldQueryParser parser = new MultiFieldQueryParser(
				org.apache.lucene.util.Version.LUCENE_48,
				cols.toArray(new String[cols.size()]), new StandardAnalyzer(
						org.apache.lucene.util.Version.LUCENE_48));

		Query q = null;
		try {
			q = parser.parse(QueryParser.escape(sb.toString()));

			// logger.log(DebugLevel.DEBUG, "Approx data matching query : " +
			// q);

		} catch (ParseException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

		return q;

	}

	public Query buildApproxMatchingBoostQuery(Map<String, String> colsToVal,
			List<String> cols) {

		QueryParser queryParser = new QueryParser(
				org.apache.lucene.util.Version.LUCENE_48, "",
				new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_48));

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cols.size(); i++) {
			String col = cols.get(i);
			if (i == cols.size() - 1) {
				String v = QueryParser.escape(colsToVal.get(col));
				sb.append(col + ":" + v + "~");
			} else {
				String v = QueryParser.escape(colsToVal.get(col));
				sb.append(col + ":" + v + " AND ");

			}
		}

		Query q = null;

		try {
			q = queryParser.parse(sb.toString());

			// logger.log(DebugLevel.DEBUG, "Approx data matching query : " +
			// q);

		} catch (ParseException e) {
			logger.log(ProdLevel.PROD, "EXCEPTION : " + e.getMessage());
		}

		return q;
	}
}
