package data.cleaning.core.service.dataset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.GroundTruthDataset;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.errgen.ErrgenService;
import data.cleaning.core.service.errgen.impl.ErrorGenStrategy;
import data.cleaning.core.service.errgen.impl.ErrorMetadata;
import data.cleaning.core.service.errgen.impl.ErrorType;
import data.cleaning.core.service.repair.RepairService;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.Config.Dataset;
import data.cleaning.core.utils.ProdLevel;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class DatasetServiceTests {

	@Autowired
	private DatasetService datasetService;
	@Autowired
	private ErrgenService errgenService;
	@Autowired
	private RepairService repairService;
	private static final Logger logger = Logger
			.getLogger(DatasetServiceTests.class);

	protected float simThreshold;

	protected String origUrl;
	protected String origName;
	protected String tgtErrUrl;
	protected String tgtErrMetaUrl;
	protected String mUrl;
	protected String gtUrl;
	protected String tgtErrName;
	protected String mName;
	protected String fdUrl;
	protected char datasetSeparator;
	protected char datasetQuoteChar;
	private Random rand = new Random(Config.SEED);

	/**
	 * Convenience method.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPrepareDatasets() throws Exception {
		testConstructGroundTruth();
		testConstructTargetErrs();
		testCorrectnessOfErrorMetadata();
		testConstructMaster();
	}

	/**
	 * Rough and dirty method.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPrepareIMDBAllDatasets() throws Exception {

		String[] origUrls = new String[] { "datasets/imdb/imdb_200k.csv",
				"datasets/imdb/imdb_400k.csv", "datasets/imdb/imdb_600k.csv",
				"datasets/imdb/imdb_800k.csv", "datasets/imdb/imdb_1m.csv",
				"datasets/imdb/imdb_1.2m.csv", };
		String[] origNames = new String[] { "imdb_200k", "imdb_400k",
				"imdb_600k", "imdb_800k", "imdb_1m", "imdb_1.2m", };

		if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.RANDOM) {
			for (int i = 0; i < origUrls.length; i++) {

				Config.imdbOrigFileUrl = origUrls[i];
				Config.imdbOrigFileName = origNames[i];
				origUrl = Config.imdbOrigFileUrl;
				origName = Config.imdbOrigFileName;

				logger.log(ProdLevel.PROD, "\nIMDB URL : " + origUrl);

				testConstructGroundTruth();
				testConstructTargetErrs();
				testCorrectnessOfErrorMetadata();
				testConstructMaster();
			}
		} else {
			// Cumulative err creation is quite different for inc tuples.
			Config.imdbOrigFileUrl = origUrls[0];
			Config.imdbOrigFileName = origNames[0];
			origUrl = Config.imdbOrigFileUrl;
			origName = Config.imdbOrigFileName;

			reloadConfigs(-1f);

			GroundTruthDataset gtDataset = datasetService
					.constructGroundTruthDataset(origUrl, tgtErrName, gtUrl,
							"", fdUrl, datasetSeparator, datasetQuoteChar);

			logger.log(ProdLevel.PROD, "\n");

			for (Constraint constraint : gtDataset.getConstraints()) {
				Assert.assertTrue(datasetService.isSatisfied(constraint,
						gtDataset.getRecords()));
			}

			// All inc tuples use the same gt file.
			for (int i = 1; i < origUrls.length; i++) {

				Config.imdbOrigFileUrl = origUrls[i];
				Config.imdbOrigFileName = origNames[i];
				origUrl = Config.imdbOrigFileUrl;
				origName = Config.imdbOrigFileName;
				reloadConfigs(Config.CONSQ_ERR_INJECT[0]);

				datasetService.saveDataset(gtDataset.getRecords(), gtUrl,
						datasetSeparator, datasetQuoteChar);

			}

			List<ErrorType> types = constructErrorDistribution();
			int[] desiredTgtRecsArr = new int[origUrls.length];

			for (int i = 0; i < origUrls.length; i++) {

				Config.imdbOrigFileUrl = origUrls[i];
				Config.imdbOrigFileName = origNames[i];
				origUrl = Config.imdbOrigFileUrl;
				origName = Config.imdbOrigFileName;

				reloadConfigs(Config.CONSQ_ERR_INJECT[0]);
				desiredTgtRecsArr[i] = datasetService
						.getTargetDatasetSize(origUrl);
			}

			String err = "";
			if (Config.CONSQ_ERR_INJECT[0] != -1f) {
				err = (int) (Config.CONSQ_ERR_INJECT[0] * 100) + "";
			}

			String ext = Config.ERR_GEN_STRATEGY == ErrorGenStrategy.RANDOM ? "_temd"
					+ err
					: "_temdc" + err;
			tgtErrMetaUrl = inject(Config.CORA_ORIG_FILE_URL, ext);

			List<String> tgtErrUrls = new ArrayList<>();
			List<String> tgtErrMetaUrls = new ArrayList<>();

			for (int i = 0; i < origUrls.length; i++) {

				Config.imdbOrigFileUrl = origUrls[i];
				Config.imdbOrigFileName = origNames[i];
				origUrl = Config.imdbOrigFileUrl;
				origName = Config.imdbOrigFileName;
				reloadConfigs(Config.CONSQ_ERR_INJECT[0]);

				tgtErrUrls.add(new String(tgtErrUrl));
				tgtErrMetaUrls.add(new String(tgtErrMetaUrl));
			}

			errgenService.addErrorsCumulativeIncTuples(types,
					desiredTgtRecsArr, Config.ROUGH_CHUNK_SIZE,
					0.20d * Config.CONSQ_ERR_INJECT[0],
					Config.CONSQ_ERR_INJECT[0], gtUrl, tgtErrUrls, fdUrl,
					tgtErrMetaUrls, datasetSeparator, datasetQuoteChar,
					Config.NUM_NON_MATCH_INC_TUPLES);

			for (int i = 0; i < origUrls.length; i++) {
				Config.imdbOrigFileUrl = origUrls[i];
				Config.imdbOrigFileName = origNames[i];
				origUrl = Config.imdbOrigFileUrl;
				origName = Config.imdbOrigFileName;
				reloadConfigs(Config.CONSQ_ERR_INJECT[0]);

				testCorrectnessOfErrorMetadata();

				int numIncorrectMatches = Config.NUM_INCORRECT_MATCHES_INC_TUPLES[i];

				MasterDataset masterDataset = datasetService
						.constructMasterDataset(gtUrl, mUrl, mName, tgtErrUrl,
								tgtErrName, -1, tgtErrMetaUrl, fdUrl,
								datasetSeparator, datasetQuoteChar,
								simThreshold, numIncorrectMatches);

				for (int j = 0; j < masterDataset.getConstraints().size(); j++) {
					Constraint constraint = masterDataset.getConstraints().get(
							j);
					logger.log(ProdLevel.PROD,
							"Master url : " + masterDataset.getUrl());
					// For debugging;
					// if (!datasetService.isSatisfied(constraint,
					// masterDataset.getRecords())) {
					//
					// Violations vs = repairService.calcViolations(
					// masterDataset.getRecords(), constraint);
					// logger.log(ProdLevel.PROD, "Viols : " + vs);
					// }

					Assert.assertTrue(datasetService.isSatisfied(constraint,
							masterDataset.getRecords()));
				}

				masterDataset = null;
			}
		}
	}

	private void reloadConfigs(float errPerc) {
		String err = "";
		if (errPerc != -1f) {
			err = (int) (errPerc * 100) + "";
		}

		String ext = Config.ERR_GEN_STRATEGY == ErrorGenStrategy.RANDOM ? "_temd"
				+ err
				: "_temdc" + err;

		if (Config.CURRENT_DATASET == Dataset.CORA) {
			origName = Config.CORA_ORIG_FILE_NAME;
			tgtErrName = Config.CORA_ORIG_FILE_NAME + "_te" + err;
			mName = Config.CORA_ORIG_FILE_NAME + "_m" + err;

			fdUrl = Config.CORA_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.CORA_SIM_THRESHOLD;

			origUrl = Config.CORA_ORIG_FILE_URL;
			mUrl = inject(Config.CORA_ORIG_FILE_URL, "_m" + err);
			gtUrl = inject(Config.CORA_ORIG_FILE_URL, "_gt");
			tgtErrUrl = inject(Config.CORA_ORIG_FILE_URL, "_te" + err);
			tgtErrMetaUrl = inject(Config.CORA_ORIG_FILE_URL, ext);
		} else if (Config.CURRENT_DATASET == Dataset.BOOKS) {
			origName = Config.booksOrigFileName;
			tgtErrName = Config.booksOrigFileName + "_te" + err;
			mName = Config.booksOrigFileName + "_m" + err;

			fdUrl = Config.BOOKS_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.BOOKS_SIM_THRESHOLD;

			origUrl = Config.booksOrigFileUrl;
			mUrl = inject(Config.booksOrigFileUrl, "_m" + err);
			gtUrl = inject(Config.booksOrigFileUrl, "_gt");
			tgtErrUrl = inject(Config.booksOrigFileUrl, "_te" + err);
			tgtErrMetaUrl = inject(Config.booksOrigFileUrl, ext);
		} else if (Config.CURRENT_DATASET == Dataset.IMDB) {
			origName = Config.imdbOrigFileName;
			tgtErrName = Config.imdbOrigFileName + "_te" + err;
			mName = Config.imdbOrigFileName + "_m" + err;

			fdUrl = Config.IMDB_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.IMDB_SIM_THRESHOLD;

			origUrl = Config.imdbOrigFileUrl;
			mUrl = inject(Config.imdbOrigFileUrl, "_m" + err);
			gtUrl = inject(Config.imdbOrigFileUrl, "_gt");
			tgtErrUrl = inject(Config.imdbOrigFileUrl, "_te" + err);
			tgtErrMetaUrl = inject(Config.imdbOrigFileUrl, ext);
		} else if (Config.CURRENT_DATASET == Dataset.POLLUTION) {
			origName = Config.POLLUTION_ORIG_FILE_NAME;
			tgtErrName = Config.POLLUTION_ORIG_FILE_NAME + "_te" + err;
			mName = Config.POLLUTION_ORIG_FILE_NAME + "_m" + err;

			fdUrl = Config.POLLUTION_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.POLLUTION_SIM_THRESHOLD;

			origUrl = Config.POLLUTION_ORIG_FILE_URL;
			mUrl = inject(Config.POLLUTION_ORIG_FILE_URL, "_m" + err);
			gtUrl = inject(Config.POLLUTION_ORIG_FILE_URL, "_gt");
			tgtErrUrl = inject(Config.POLLUTION_ORIG_FILE_URL, "_te" + err);
			tgtErrMetaUrl = inject(Config.POLLUTION_ORIG_FILE_URL, ext);
		} else {
			origName = Config.HEALTH_ORIG_FILE_NAME;
			tgtErrName = Config.HEALTH_ORIG_FILE_NAME + "_te" + err;
			mName = Config.HEALTH_ORIG_FILE_NAME + "_m" + err;

			fdUrl = Config.HEALTH_ORIG_FD_URL;
			datasetQuoteChar = Config.DATASET_DOUBLE_QUOTE_CHAR;
			datasetSeparator = Config.DATASET_SEPARATOR;
			simThreshold = Config.HEALTH_SIM_THRESHOLD;

			origUrl = Config.HEALTH_ORIG_FILE_URL;
			mUrl = inject(Config.HEALTH_ORIG_FILE_URL, "_m" + err);
			gtUrl = inject(Config.HEALTH_ORIG_FILE_URL, "_gt");
			tgtErrUrl = inject(Config.HEALTH_ORIG_FILE_URL, "_te" + err);
			tgtErrMetaUrl = inject(Config.HEALTH_ORIG_FILE_URL, ext);

		}
	}

	private String inject(String origFileUrl, String extension) {
		String prefix = origFileUrl.substring(0, origFileUrl.length() - 4);
		return prefix + extension + ".csv";
	}

	// public void flexiUrls(double percConsErr) {
	// flexiTgtErrUrl = tgtErrUrl.replaceFirst("%s", ""
	// + (int) (percConsErr * 100.0d));
	//
	// flexiTgtErrMetaUrl = tgtErrMetaUrl.replaceFirst("%s", ""
	// + (int) (percConsErr * 100.0d));
	// }

	@Test
	public void testConstructGroundTruth() throws Exception {
		reloadConfigs(-1f);

		GroundTruthDataset gtDataset = datasetService
				.constructGroundTruthDataset(origUrl, tgtErrName, gtUrl, "",
						fdUrl, datasetSeparator, datasetQuoteChar);

		logger.log(ProdLevel.PROD, "\n");

		for (Constraint constraint : gtDataset.getConstraints()) {
			Assert.assertTrue(datasetService.isSatisfied(constraint,
					gtDataset.getRecords()));
		}

	}

	@Test
	public void testConstructTargetErrs() throws Exception {
		if (Config.ERR_GEN_STRATEGY == ErrorGenStrategy.RANDOM) {
			addErrorsRandom();
		} else {
			addErrorsCumulative();
		}
	}

	private void addErrorsCumulative() throws Exception {
		List<ErrorType> types = constructErrorDistribution();

		int desiredTgtRecs = datasetService.getTargetDatasetSize(origUrl);

		double[] percAntsErrs = new double[Config.CONSQ_ERR_INJECT.length];
		double[] percConsErrs = new double[Config.CONSQ_ERR_INJECT.length];
		List<String> tgtErrUrls = new ArrayList<>();
		List<String> tgtErrMetaUrls = new ArrayList<>();

		for (int i = 0; i < Config.CONSQ_ERR_INJECT.length; i++) {
			float percConsErr = Config.CONSQ_ERR_INJECT[i];

			reloadConfigs(percConsErr);
			percAntsErrs[i] = 0.20d * percConsErr;
			percConsErrs[i] = percConsErr;
			tgtErrUrls.add(new String(tgtErrUrl));
			tgtErrMetaUrls.add(new String(tgtErrMetaUrl));
		}

		errgenService.addErrorsCumulative(types, desiredTgtRecs,
				Config.ROUGH_CHUNK_SIZE, percAntsErrs, percConsErrs, gtUrl,
				tgtErrUrls, fdUrl, tgtErrMetaUrls, datasetSeparator,
				datasetQuoteChar, Config.NUM_NON_MATCH);

		testCheckNumErrors();
	}

	@Test
	public void testCheckNumErrors() {
		for (int i = 0; i < Config.CONSQ_ERR_INJECT.length; i++) {
			float percConsErr = Config.CONSQ_ERR_INJECT[i];

			reloadConfigs(percConsErr);
			TargetDataset wErrors = datasetService.loadTargetDataset(tgtErrUrl,
					tgtErrMetaUrl, fdUrl, datasetSeparator, datasetQuoteChar);
			int numChunks = 0;
			int totViols = 0;
			for (int j = 0; j < wErrors.getConstraints().size(); j++) {
				Constraint constraint = wErrors.getConstraints().get(j);
				Violations v = repairService.calcViolations(
						wErrors.getRecords(), constraint);
				totViols += v.getViolMap().size();
				numChunks += v.getViolMap().keySet().size();
				logger.log(ProdLevel.PROD, "Constraint : " + constraint
						+ ", Num chunks : " + v.getViolMap().keySet().size());
			}

			// logger.log(ProdLevel.PROD, "Tot viols after : " + totViols);

			double percCons = (double) totViols
					/ (double) wErrors.getRecords().size();

			logger.log(ProdLevel.PROD, "\nChecking percentage consq errors : "
					+ percCons + " (tot viols : " + totViols
					+ "), desired errors : " + percConsErr);

			Assert.assertTrue(Math.abs(percCons - percConsErr) < 0.005);

			logger.log(ProdLevel.PROD, "Num chunks : " + numChunks);
			logger.log(ProdLevel.PROD, "Avg viols per chunk : "
					+ (float) totViols / (float) numChunks + "\n");

			wErrors = null;
		}
	}

	@Test
	public void testPrintStatsAboutErrors() {
		for (int i = 0; i < Config.CONSQ_ERR_INJECT.length; i++) {
			float percConsErr = Config.CONSQ_ERR_INJECT[i];

			reloadConfigs(percConsErr);
			TargetDataset wErrors = datasetService.loadTargetDataset(tgtErrUrl,
					tgtErrMetaUrl, fdUrl, datasetSeparator, datasetQuoteChar);
			int numChunks = 0;
			int totViols = 0;
			for (int j = 0; j < wErrors.getConstraints().size(); j++) {
				Constraint constraint = wErrors.getConstraints().get(j);
				Violations v = repairService.calcViolations(
						wErrors.getRecords(), constraint);
				totViols += v.getViolMap().size();
				numChunks += v.getViolMap().keySet().size();

				logger.log(ProdLevel.PROD, "Constraint : " + constraint
						+ ", Viol map : " + v.getViolMap());
				logger.log(ProdLevel.PROD, "Constraint : " + constraint
						+ ", Num chunks : " + v.getViolMap().keySet().size());

			}

			logger.log(ProdLevel.PROD, "Num chunks : " + numChunks);
			logger.log(ProdLevel.PROD, "Avg viols per chunk : "
					+ (float) totViols / (float) numChunks + "\n");
		}
	}

	private void addErrorsRandom() throws Exception {
		List<ErrorType> types = constructErrorDistribution();

		int desiredTgtRecs = datasetService.getTargetDatasetSize(origUrl);

		for (float percConsErr : Config.CONSQ_ERR_INJECT) {
			reloadConfigs(percConsErr);

			TargetDataset wErrors = errgenService.addErrorsRandom(types,
					desiredTgtRecs, Config.ROUGH_CHUNK_SIZE,
					0.20 * percConsErr, percConsErr, gtUrl, tgtErrUrl,
					tgtErrName, fdUrl, tgtErrMetaUrl, datasetSeparator,
					datasetQuoteChar);

			int totViols = 0;
			for (int i = 0; i < wErrors.getConstraints().size(); i++) {
				Constraint constraint = wErrors.getConstraints().get(i);
				Violations v = repairService.calcViolations(
						wErrors.getRecords(), constraint);
				totViols += v.getViolMap().size();
			}

			// logger.log(ProdLevel.PROD, "Tot viols after : " + totViols);

			double percCons = (double) totViols
					/ (double) wErrors.getRecords().size();

			logger.log(ProdLevel.PROD, "\nChecking percentage consq errors : "
					+ percCons);

			Assert.assertTrue(Math.abs(percCons - percConsErr) < 0.005);

			wErrors = null;
		}
	}

	@Test
	public void testConstructMaster() throws Exception {

		for (int i = 0; i < Config.CONSQ_ERR_INJECT.length; i++) {
			float percConsErr = Config.CONSQ_ERR_INJECT[i];
			int numIncorrectMatches = Config.NUM_INCORRECT_MATCHES[i];
			reloadConfigs(percConsErr);

			MasterDataset masterDataset = datasetService
					.constructMasterDataset(gtUrl, mUrl, mName, tgtErrUrl,
							tgtErrName, -1, tgtErrMetaUrl, fdUrl,
							datasetSeparator, datasetQuoteChar, simThreshold,
							numIncorrectMatches);

			for (int j = 0; j < masterDataset.getConstraints().size(); j++) {
				Constraint constraint = masterDataset.getConstraints().get(j);
				logger.log(ProdLevel.PROD,
						"Master url : " + masterDataset.getUrl());
				// For debugging;
				// if (!datasetService.isSatisfied(constraint,
				// masterDataset.getRecords())) {
				//
				// Violations vs = repairService.calcViolations(
				// masterDataset.getRecords(), constraint);
				// logger.log(ProdLevel.PROD, "Viols : " + vs);
				// }

				Assert.assertTrue(datasetService.isSatisfied(constraint,
						masterDataset.getRecords()));
			}

			masterDataset = null;
		}
	}

	public List<ErrorType> constructErrorDistribution() {
		float belowThresholdDistr = Config.BELOW_THRESHOLD_DISTR;
		float aboveThresholdDistr = 1f - belowThresholdDistr;

		List<ErrorType> types = new ArrayList<>();

		ErrorType e1 = new ErrorType(ErrorType.Type.IN_DOMAIN_SIMILAR, 0.49f);
		Map<Float, Float> simToDistribution = new LinkedHashMap<>();
		int numSteps = (int) (aboveThresholdDistr / 0.1f);
		float diff = 1f - (simThreshold + Config.FLOAT_EQUALIY_EPSILON);

		for (int i = 0; i < numSteps; i++) {
			simToDistribution.put(simThreshold + (diff * rand.nextFloat()),
					0.1f);
		}

		simToDistribution.put(simThreshold * 0.8f, belowThresholdDistr);
		e1.setSimToDistribution(simToDistribution);
		ErrorType e2 = new ErrorType(ErrorType.Type.IN_DOMAIN, 0.01f);
		ErrorType e3 = new ErrorType(ErrorType.Type.OUTSIDE_DOMAIN, 0.01f);
		ErrorType e4 = new ErrorType(ErrorType.Type.OUTSIDE_DOMAIN_SIMILAR,
				0.44f);
		e4.setSimToDistribution(new HashMap<>(simToDistribution));
		ErrorType e5 = new ErrorType(ErrorType.Type.SPECIAL_CHARS, 0.05f);
		List<String> specialChars = new ArrayList<>();
		specialChars.add("!");
		specialChars.add("#");
		specialChars.add("@");
		e5.setSpecialChars(specialChars);
		types.add(e1);
		types.add(e2);
		types.add(e3);
		types.add(e4);
		types.add(e5);
		return types;
	}

	@Test
	public void testCorrectnessOfErrorMetadata() throws Exception {

		for (float percConsErr : Config.CONSQ_ERR_INJECT) {
			reloadConfigs(percConsErr);
			List<ErrorMetadata> emds = datasetService.loadErrMetadata(
					tgtErrMetaUrl, datasetSeparator, datasetQuoteChar);
			TargetDataset tgtDataset = datasetService.loadTargetDataset(
					tgtErrUrl, tgtErrMetaUrl, fdUrl, datasetSeparator,
					datasetQuoteChar);

			for (ErrorMetadata emd : emds) {
				Record error = tgtDataset.getRecord(emd.getTid());
				Map<String, String> origColsToVal = emd.getOrigColsToVal();

				for (Map.Entry<String, String> e : origColsToVal.entrySet()) {
					error.modifyValForExistingCol(e.getKey(), e.getValue());
				}
			}

			logger.log(ProdLevel.PROD, "Checking emd : " + tgtErrMetaUrl);

			for (Constraint constraint : tgtDataset.getConstraints()) {
				// For debugging;
				// if (!datasetService.isSatisfied(constraint,
				// tgtDataset.getRecords())) {
				//
				// Violations vs = repairService.calcViolations(
				// tgtDataset.getRecords(), constraint);
				// logger.log(ProdLevel.PROD, "Viols : " + vs);
				//
				// Multimap<String, Record> vm = vs.getViolMap();
				// for (String s : vm.keySet()) {
				// Collection<Record> rs = vm.get(s);
				//
				// Set<Long> rids = getRids(rs);
				// for (ErrorMetadata emd : emds) {
				// if (rids.contains(emd.getTid())) {
				// logger.log(ProdLevel.PROD, "Emd : " + emd);
				//
				// }
				// }
				//
				// }
				// }

				Assert.assertTrue(datasetService.isSatisfied(constraint,
						tgtDataset.getRecords()));

			}

		}
	}

	public Set<Long> getRids(Collection<Record> records) {
		Set<Long> rids = new HashSet<>();

		for (Record record : records) {
			rids.add(record.getId());
		}

		return rids;
	}

	/**
	 * Quick and dirty method. Split up the location column into 3 separate
	 * columns.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProprocessBooksDataset() throws Exception {
		TargetDataset tgtDataset = datasetService.loadTargetDataset(
				"datasets/books/books_raw_1m.csv", Config.booksOrigFileName,
				Config.BOOKS_ORIG_FD_URL, Config.DATASET_SEPARATOR,
				Config.DATASET_DOUBLE_QUOTE_CHAR);

		for (Record r : tgtDataset.getRecords()) {
			Map<String, String> cToV = r.getColsToVal();
			String s = cToV.get("user_location");

			if (s != null && !s.isEmpty()) {
				String[] location = s.split(",");
				for (int i = 0; i < location.length; i++) {
					String details = location[i].trim();
					if (details == null || details.isEmpty()) {
						details = "";
					}
					if (i == 0) {
						cToV.put("city", details);
					} else if (i == 1) {
						cToV.put("state", details);
					} else {
						cToV.put("country", details);
					}
				}

			} else {
				cToV.put("city", "");
				cToV.put("state", "");
				cToV.put("country", "");
			}

			cToV.remove("user_location");
		}

		datasetService.saveDataset(tgtDataset.getRecords(),
				"datasets/books/books_1m.csv", Config.DATASET_SEPARATOR,
				Config.DATASET_DOUBLE_QUOTE_CHAR);
	}

	/**
	 * Quick and dirty method. Split up the location column into 3 separate
	 * columns.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testIncRecsBooksDataset() throws Exception {
		TargetDataset tgtDataset = datasetService.loadTargetDataset(
				"datasets/books/books_raw_1m.csv", Config.booksOrigFileName,
				Config.BOOKS_ORIG_FD_URL, Config.DATASET_SEPARATOR,
				Config.DATASET_DOUBLE_QUOTE_CHAR);

		String[] origUrls = new String[] { "datasets/books/books_500k.csv",
				"datasets/books/books_1m.csv", "datasets/books/books_1.5m.csv",
				"datasets/books/books_2m.csv", "datasets/books/books_2.5m.csv",
				"datasets/books/books_3m.csv" };
		List<Record> tgtRecs = tgtDataset.getRecords();
		for (int i = 0; i < origUrls.length; i++) {
			List<Record> toSave = new ArrayList<>();
			int toAdd = 0;
			if (i == 0) {
				toAdd = 500000;
			} else if (i == 1) {
				toAdd = 1000000;
			} else if (i == 2) {
				toAdd = 1500000;
			} else if (i == 3) {
				toAdd = 2000000;
			} else if (i == 4) {
				toAdd = 2500000;
			} else if (i == 5) {
				toAdd = 3000000;
			}

			while (toSave.size() < toAdd) {
				Record r = tgtRecs.get(rand.nextInt(tgtRecs.size()));
				toSave.add(r);
			}

			datasetService.saveDataset(toSave, origUrls[i],
					Config.DATASET_SEPARATOR, Config.DATASET_DOUBLE_QUOTE_CHAR);
		}

	}

	/**
	 * Rough and dirty method.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPrepBooksForScalability() throws Exception {

		String[] origUrls = new String[] { "datasets/books/books_500k.csv",
				"datasets/books/books_1m.csv", "datasets/books/books_1.5m.csv",
				"datasets/books/books_2m.csv", "datasets/books/books_2.5m.csv",
				"datasets/books/books_3m.csv" };
		String[] origNames = new String[] { "books_500k", "books_1m",
				"books_1.5m", "books_2m", "books_2.5m", "books_3m" };

		for (int i = 0; i < origUrls.length; i++) {

			Config.booksOrigFileUrl = origUrls[i];
			Config.booksOrigFileName = origNames[i];
			origUrl = Config.booksOrigFileUrl;
			origName = Config.booksOrigFileName;

			logger.log(ProdLevel.PROD, "\nBOOKS URL : " + origUrl);

			testConstructGroundTruth();
			testConstructTargetErrs();
			testCorrectnessOfErrorMetadata();
			testConstructMaster();
		}

	}
}
