package data.cleaning.core.service.matching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import data.cleaning.core.DataCleaningTests;
import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.errgen.impl.ErrorMetadata;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.RepairService;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.ProdLevel;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class QMatchingServiceTests extends DataCleaningTests {
	@Autowired
	private MatchingService matchingService;
	@Autowired
	private DatasetService datasetService;
	@Autowired
	private RepairService repairService;

	private static final Logger logger = Logger
			.getLogger(QMatchingServiceTests.class);

	@Test
	public void testPvtMatching() throws Exception {
		List<Match> tgtMatches = matchingService.applyPvtApproxDataMatching(
				tgtDataset.getConstraints().get(1), tgtDataset.getRecords()
						.subList(200, 210), mDataset.getRecords(),
				Config.NUM_STRINGS, Config.DIM_REDUCTION, simThreshold,
				Config.SHOULD_APPROX_DIST, Config.SHOULD_AVERAGE, tgtDataset
						.getName(), mDataset.getName());

		logger.log(ProdLevel.PROD, "tgtMatches : " + tgtMatches);

	}

	@Test
	public void testMatching() throws Exception {
		List<Match> tgtMatches = matchingService.applyApproxDataMatching(
				tgtDataset.getConstraints().get(1), tgtDataset.getRecords()
						.subList(200, 210), mDataset.getRecords(),
				simThreshold, tgtDataset.getName(), mDataset.getName());

		logger.log(ProdLevel.PROD, "tgtMatches : " + tgtMatches);

	}

	@Test
	public void testCompareMatching() throws Exception {
		List<Constraint> constraints = tgtDataset.getConstraints();

		for (Constraint constraint : constraints) {
			Violations v = repairService.calcViolations(
					tgtDataset.getRecords(), constraint);

			List<Record> viols = new ArrayList<>(v.getViolMap().values());

			List<Match> tgtMatches = matchingService.applyApproxDataMatching(
					constraint, viols, mDataset.getRecords(), simThreshold,
					tgtDataset.getName(), mDataset.getName());

			// List<Match> tgtMatches2 = matchingService
			// .applyPvtApproxDataMatching(constraint, viols,
			// mDataset.getRecords(), Config.NUM_STRINGS,
			// Config.DIM_REDUCTION, simThreshold,
			// Config.SHOULD_APPROX_DIST, Config.SHOULD_AVERAGE,
			// tgtDataset.getName(), mDataset.getName());

			// logger.log(ProdLevel.PROD, "tgtMatches : " + tgtMatches);
			// logger.log(ProdLevel.PROD, "tgtMatches2 : " + tgtMatches2);

			List<ErrorMetadata> emds = datasetService.loadErrMetadata(
					tgtErrMetaUrl, datasetSeparator, datasetQuoteChar);

		}

	}

}
