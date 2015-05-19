package data.cleaning.core.utils;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import data.cleaning.core.DataCleaningTests;
import data.cleaning.core.service.dataset.DatasetService;
import data.cleaning.core.service.dataset.impl.InfoContentTable;

/**
 * Any miscellaneous tests can be added here.
 * 
 * @author dhruvgairola
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class UtilsTests extends DataCleaningTests {
	@Autowired
	private DatasetService datasetService;

	private static final Logger logger = Logger.getLogger(UtilsTests.class);

	@Test
	public void testInfoContentTable() throws Exception {

		InfoContentTable table2 = datasetService.calcInfoContentTable(mDataset
				.getConstraints().get(0), mDataset);
		double[][] data2 = table2.getData();
		for (double[] d : data2) {
			logger.log(ProdLevel.PROD, Arrays.toString(d));
		}
	}

	@Test
	public void testInd() throws Exception {
		logger.log(
				ProdLevel.PROD,
				"Ind : "
						+ Stats.ind(tgtDataset.getConstraints().get(0),
								tgtDataset.getRecords()));

	}
}
