package data.cleaning.core;

import static org.junit.Assert.assertNotNull;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.errgen.impl.ErrorGenStrategy;
import data.cleaning.core.service.matching.MatchingServiceTests;
import data.cleaning.core.service.repair.RepairService;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.Config.Dataset;
import data.cleaning.core.utils.ProdLevel;

/**
 * Misc tests can be added here.
 * 
 * @author dhruvgairola
 * 
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class DataCleaningTests {

	@Autowired
	private DatasetService datasetService;
	@Autowired
	private RepairService repairService;
	protected TargetDataset tgtDataset;
	protected MasterDataset mDataset;
	protected List<Constraint> constraints;
	protected Random rand;
	protected float simThreshold;
	protected String tgtErrUrl;
	protected String mUrl;
	protected String gtUrl;
	protected String tgtErrName;
	protected String mName;
	protected String fdUrl;
	protected String origName;
	protected String origUrl;
	protected String tgtErrMetaUrl;
	protected char datasetSeparator;
	protected char datasetQuoteChar;
	private static final Logger logger = Logger
			.getLogger(MatchingServiceTests.class);

	@Before
	public void initEnv() throws Exception {
		loadConfigs(Config.CONSQ_ERR_INJECT[0]);
		loadDatsets(Config.CONSQ_ERR_INJECT[0]);
	}

	public void loadConfigs(float errPerc) {
		String err = (int) (errPerc * 100) + "";
		if (Config.CURRENT_DATASET == Dataset.CORA) {
			origName = Config.CORA_ORIG_FILE_NAME;
			origUrl = Config.CORA_ORIG_FILE_URL;

			fdUrl = Config.CORA_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.CORA_SIM_THRESHOLD;

		} else if (Config.CURRENT_DATASET == Dataset.BOOKS) {
			origName = Config.booksOrigFileName;
			origUrl = Config.booksOrigFileUrl;

			fdUrl = Config.BOOKS_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.BOOKS_SIM_THRESHOLD;

		} else if (Config.CURRENT_DATASET == Dataset.IMDB) {
			origName = Config.imdbOrigFileName;
			origUrl = Config.imdbOrigFileUrl;

			fdUrl = Config.IMDB_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.IMDB_SIM_THRESHOLD;

		} else if (Config.CURRENT_DATASET == Dataset.POLLUTION) {
			origName = Config.POLLUTION_ORIG_FILE_NAME;
			origUrl = Config.POLLUTION_ORIG_FILE_URL;

			fdUrl = Config.POLLUTION_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.POLLUTION_SIM_THRESHOLD;

		} else if (Config.CURRENT_DATASET == Dataset.HEALTH) {
			origName = Config.HEALTH_ORIG_FILE_NAME;
			origUrl = Config.HEALTH_ORIG_FILE_URL;

			fdUrl = Config.HEALTH_ORIG_FD_URL;
			datasetQuoteChar = (char) -1;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.HEALTH_SIM_THRESHOLD;
		}

		tgtErrName = origName + "_te" + err;
		mName = origName + "_m" + err;

		String ext = Config.ERR_GEN_STRATEGY == ErrorGenStrategy.RANDOM ? "_temd"
				+ err
				: "_temdc" + err;
		tgtErrUrl = inject(origUrl, "_te" + err);
		mUrl = inject(origUrl, "_m" + err);
		gtUrl = inject(origUrl, "_gt");
		tgtErrMetaUrl = inject(origUrl, ext);
	}

	public void loadDatsets(float errPerc) throws Exception {

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));
		logger.log(ProdLevel.PROD, "\nTime now : " + dateFormat.format(date));

		logger.log(ProdLevel.PROD, "\nConstructing datasets \n");

		rand = new Random(Config.SEED);

		tgtDataset = datasetService.loadTargetDataset(tgtErrUrl, tgtErrName,
				fdUrl, datasetSeparator, datasetQuoteChar);

		mDataset = datasetService.loadMasterDataset(mUrl, mName, fdUrl,
				tgtDataset.getId(), datasetSeparator, datasetQuoteChar);

		constraints = new ArrayList<>();

		List<Violations> orderedViolations = repairService
				.orderViolations(tgtDataset);

		for (Violations v : orderedViolations) {
			constraints.add(v.getConstraint());
		}

		logger.log(ProdLevel.PROD, "ORDERED : " + constraints);

		logger.log(ProdLevel.PROD, "\nFinished constructing datasets \n");

		assertNotNull(tgtDataset);
		assertNotNull(mDataset);
		assertNotNull(constraints);

		logger.log(ProdLevel.PROD, "\n-----------------------------------");
		logger.log(ProdLevel.PROD, "INITIAL ENVIRONMENT");
		logger.log(ProdLevel.PROD, "\n-----------------------------------");
		logger.log(ProdLevel.PROD, "\nTgt err : " + errPerc + "\n");
		logger.log(ProdLevel.PROD, "\nConstraints : \n");

		for (Constraint constraint : constraints) {
			logger.log(ProdLevel.PROD, constraint);

			if (!datasetService.isSatisfied(constraint, mDataset.getRecords())) {
				throw new Exception(
						"Master dataset does not satisfy constraint "
								+ constraint
								+ ". This is not allowed in our protocol.");
			}

		}

		if (tgtDataset.getRecords().size() < 100
				&& mDataset.getRecords().size() < 100) {
			logger.log(ProdLevel.PROD, "\nInitial master instance : \n");

			List<Record> mRecords = mDataset.getRecords();

			for (Record rep : mRecords) {
				logger.log(ProdLevel.PROD, rep.prettyPrintRecord(null));
			}

			logger.log(ProdLevel.PROD, "\nInitial target instance : \n");

			List<Record> tgtRecords = tgtDataset.getRecords();

			for (Record rep : tgtRecords) {
				logger.log(ProdLevel.PROD, rep.prettyPrintRecord(null));
			}
		}
	}

	private String inject(String origFileUrl, String extension) {
		String prefix = origFileUrl.substring(0, origFileUrl.length() - 4);
		return prefix + extension + ".csv";
	}

}
