/**
 */
package data.cleaning.core.service.errgen;

import java.util.List;

import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.errgen.impl.ErrorType;

public interface ErrgenService {

	/**
	 * Complicated implementation. Only used for preprocessing to generate
	 * datasets with errors, hence not completely optimized for speed.
	 * 
	 * @param types
	 *            - types of errors
	 * @param desiredTgtSize
	 *            - how many records do you desire in the output file?
	 * @param roughChunkSize
	 *            Chunk size is the size of a violation chunk (LHS is the same
	 *            and RHS is different). Choose wisely bec. for simul annealing,
	 *            you will need to increase num iterations if chunk is larger.
	 * @param percentageAnt
	 *            - some small percentage of records should have errors in the
	 *            antecedent.
	 * @param percentageCons
	 *            - control the amount of errors in the consequent.
	 * @param gtUrl
	 *            - records with no errors in them.
	 * @param tgtOutUrl
	 * @param tgtOutName
	 * @param fdUrl
	 * @param errMetadataUrl
	 * @param separator
	 * @param quoteChar
	 * @return
	 * @throws Exception
	 */
	TargetDataset addErrorsRandom(List<ErrorType> types, int desiredTgtSize,
			int roughChunkSize, double percentageAnt, double percentageCons,
			String gtUrl, String tgtOutUrl, String tgtOutName, String fdUrl,
			String errMetadataUrl, char separator, char quoteChar)
			throws Exception;

	/**
	 * Adds errors that are cumulative across the various percentages. The
	 * highest percentage err is used as the prototype which contains all the
	 * cumulative errors while the lower percentages are subsets of this. Also,
	 * no target dataset is returned since all the datasets are created at once
	 * and returning all of them as a list at once would take up too much
	 * memory.
	 * 
	 * @param types
	 * @param desiredTgtSize
	 * @param roughChunkSize
	 * @param percentageAnt
	 * @param percentageCons
	 * @param gtUrl
	 * @param tgtOutUrls
	 * @param fdUrl
	 * @param errMetadataUrls
	 * @param separator
	 * @param quoteChar
	 * @param numNonMatch
	 * @throws Exception
	 */
	void addErrorsCumulative(List<ErrorType> types, int desiredTgtSize,
			int roughChunkSize, double[] percentageAnt,
			double[] percentageCons, String gtUrl, List<String> tgtOutUrls,
			String fdUrl, List<String> errMetadataUrls, char separator,
			char quoteChar, int[] numNonMatch) throws Exception;

	/**
	 * Use this for building cumulative error tgt datasets for increasing
	 * tuples.
	 * 
	 * @param types
	 * @param desiredTgtSizes
	 * @param roughChunkSizeSmallest
	 *            - this is the rough chunk size of the smallest dataset.
	 * @param percentageAnt
	 * @param percentageCons
	 * @param gtUrl
	 * @param tgtOutUrls
	 * @param fdUrl
	 * @param errMetadataUrls
	 * @param separator
	 * @param quoteChar
	 * @param numNonMatch
	 * @throws Exception
	 */
	void addErrorsCumulativeIncTuples(List<ErrorType> types,
			int[] desiredTgtSizes, int roughChunkSizeSmallest,
			double percentageAnt, double percentageCons, String gtUrl,
			List<String> tgtOutUrls, String fdUrl,
			List<String> errMetadataUrls, char separator, char quoteChar,
			int[] numNonMatch) throws Exception;
}
