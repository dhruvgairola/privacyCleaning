package data.cleaning.core.service.matching;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import data.cleaning.core.DataCleaningTests;
import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.ProdLevel;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class MatchingServiceTests extends DataCleaningTests {
	@Autowired
	private MatchingService matchingService;
	@Autowired
	private DatasetService datasetService;

	private static final Logger logger = Logger
			.getLogger(MatchingServiceTests.class);

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

}
