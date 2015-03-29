package data.cleaning.core.service.matching;

import java.util.List;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.matching.impl.EmbVector;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.utils.Pair;

public interface MatchingService {
	List<List<String>> getRefSets(List<String> generator);

	List<String> getRandomStrings(int strLen, int numStrs, boolean isNumeric);

	float dist(String s, List<String> refSet);

	List<List<EmbVector>> embedRefSets(List<List<String>> refSets);

	EmbVector dist(String s, String meta, List<List<String>> refSets,
			List<List<EmbVector>> embRefSets, boolean shouldApproxDist);

	Pair<List<EmbVector>, List<Integer>> greedyResamplingCol(
			List<EmbVector> vectors, int reducedDim);

	/**
	 * Non-private string-similarity based matching.
	 * 
	 * @param constraint
	 * @param tgtRecords
	 * @param mRecords
	 * @param simThreshold
	 * @param tgtFileName
	 * @param mFileName
	 * @return
	 */
	List<Match> applyApproxDataMatching(Constraint constraint,
			List<Record> tgtRecords, List<Record> mRecords, float simThreshold,
			String tgtFileName, String mFileName);

	/**
	 * Private embedding based matching.
	 * 
	 * @param constraint
	 * @param tgtRecords
	 * @param mRecords
	 * @param numStrs
	 * @param dimReduction
	 * @param simThreshold
	 * @param shouldApproxDist
	 * @param shouldAvg
	 *            -If false, then decision rule matching is done for every
	 *            attribute. If true, then the whole record is used for matching
	 *            as opposed to attribute by attribute- this is much faster.
	 * @param tgtFileName
	 * @param mFileName
	 * @return
	 */
	List<Match> applyPvtApproxDataMatching(Constraint constraint,
			List<Record> tgtRecords, List<Record> mRecords, int numStrs,
			double dimReduction, float simThreshold, boolean shouldApproxDist,
			boolean shouldAvg, String tgtFileName, String mFileName);

}
