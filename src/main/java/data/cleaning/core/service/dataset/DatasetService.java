/**
 */
package data.cleaning.core.service.dataset;

import java.util.List;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.GroundTruthDataset;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.errgen.impl.ErrorMetadata;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.IndexType;

public interface DatasetService {
	TargetDataset loadTargetDataset(String tgtUrl, String tgtFileName,
			String fdUrl, char separator, char quoteChar);

	MasterDataset loadMasterDataset(String mUrl, String mFileName,
			String fdUrl, long targetId, char separator, char quoteChar);

	GroundTruthDataset loadGroundTruthDataset(String gtUrl, String gtFileName,
			String fdUrl, long targetId, char separator, char quoteChar);

	List<ErrorMetadata> loadErrMetadata(String errMetadataUrl, char separator,
			char quoteChar);

	/**
	 * Use the target dataset to build the ground truth dataset, which is a
	 * subset of the target dataset where no record violates the list of
	 * constraints.
	 * 
	 * @param tgtUrl
	 * @param tgtName
	 * @param mUrl
	 * @param mName
	 * @param fdUrl
	 * @param separator
	 * @param quoteChar
	 * @param numMaster
	 * @return
	 */
	GroundTruthDataset constructGroundTruthDataset(String tgtUrl,
			String tgtName, String gtUrl, String gtName, String fdUrl,
			char separator, char quoteChar);

	MasterDataset constructMasterDataset(String gtUrl, String mUrl,
			String mName, String tgtUrl, String tgtName, long targetId,
			String errMetadataUrl, String fdUrl, char separator,
			char quoteChar, float simThreshold, int numIncorrectMatches);

	InfoContentTable calcInfoContentTable(Constraint constraint,
			MasterDataset mDataset, IndexType type);

	boolean isSatisfied(Constraint constraint, List<Record> recs);

	void saveDataset(List<Record> records, String outputUrl, char separator,
			char quoteChar);

	void removeInfoContentUsingCandidate(InfoContentTable table,
			MasterDataset mDataset, List<Recommendation> recs);

	void saveMatchesJSON(List<Match> matches, String fileName);

	List<Match> loadMatchesJSON(String fileName);

	int getTargetDatasetSize(String tgtUrl);
}
