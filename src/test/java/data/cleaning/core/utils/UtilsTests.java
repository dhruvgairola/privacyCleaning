package data.cleaning.core.utils;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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

	private static final Logger logger = Logger
			.getLogger(UtilsTests.class);

	@Test
	public void testInfoContentTable() throws Exception {

		long start = System.nanoTime();

		InfoContentTable table = datasetService.calcInfoContentTable(mDataset
				.getConstraints().get(0), mDataset, IndexType.LUCENE);
		double[][] data = table.getData();
		for (double[] d : data) {
			logger.log(ProdLevel.PROD, Arrays.toString(d));
		}

		long end = System.nanoTime();

		long start2 = System.nanoTime();

		InfoContentTable table2 = datasetService.calcInfoContentTable(mDataset
				.getConstraints().get(0), mDataset, IndexType.HASH_MAP);
		double[][] data2 = table2.getData();
		for (double[] d : data2) {
			logger.log(ProdLevel.PROD, Arrays.toString(d));
		}

		long end2 = System.nanoTime();

		logger.log(ProdLevel.PROD, "time taken (sec) lucene : "
				+ TimeUnit.SECONDS.convert(end - start, TimeUnit.NANOSECONDS));

		logger.log(ProdLevel.PROD, "time taken (sec) hash map : "
				+ TimeUnit.SECONDS.convert(end2 - start2, TimeUnit.NANOSECONDS));

		diffInfoContent(data, data2);
	}

	private void diffInfoContent(double[][] data, double[][] data2) {
		double d = 0d;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				d += Math.abs(data[i][j] - data2[i][j]);
			}
		}

		logger.log(ProdLevel.PROD, "diff in info content : " + d);

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
