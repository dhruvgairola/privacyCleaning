package data.cleaning.core.service.repair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Before;
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
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.errgen.impl.ErrorGenStrategy;
import data.cleaning.core.service.errgen.impl.ErrorMetadata;
import data.cleaning.core.service.matching.MatchingService;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.service.repair.impl.Violations;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.IndexType;
import data.cleaning.core.utils.Metrics;
import data.cleaning.core.utils.Pair;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.Stats;
import data.cleaning.core.utils.objectives.ChangesObjective;
import data.cleaning.core.utils.objectives.CustomCleaningObjective;
import data.cleaning.core.utils.objectives.Objective;
import data.cleaning.core.utils.objectives.PrivacyObjective;
import data.cleaning.core.utils.search.HillClimbingEps;
import data.cleaning.core.utils.search.HillClimbingEpsDynamic;
import data.cleaning.core.utils.search.HillClimbingEpsLex;
import data.cleaning.core.utils.search.HillClimbingWeighted;
import data.cleaning.core.utils.search.RandomWalk;
import data.cleaning.core.utils.search.RandomWalkStats;
import data.cleaning.core.utils.search.Search;
import data.cleaning.core.utils.search.SearchType;
import data.cleaning.core.utils.search.SimulAnnealEps;
import data.cleaning.core.utils.search.SimulAnnealEpsDynamic;
import data.cleaning.core.utils.search.SimulAnnealEpsFlexible;
import data.cleaning.core.utils.search.SimulAnnealEpsLex;
import data.cleaning.core.utils.search.SimulAnnealWeighted;

/**
 * Quality tests (accuracy).
 * 
 * @author dhruvgairola
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class QRepairServiceTests extends DataCleaningTests {

	@Autowired
	private RepairService repairService;
	@Autowired
	private MatchingService matchingService;
	@Autowired
	private DatasetService datasetService;
	private Target t;
	private ThirdParty w;
	private Master m;

	private static final Logger logger = Logger
			.getLogger(QRepairServiceTests.class);

	@Before
	public void init() {
		t = new Target();
		w = new ThirdParty();
		m = new Master();
	}

	@Test
	public void testMatchQuality() throws Exception {
		Config.shdPartitionViols = false;
		runProtocolEVS(SearchType.SA_WEIGHTED, true);
	}

	@Test
	public void testHillClimbingEps() throws Exception {
		runProtocolEVS(SearchType.HC_EPS, false);
	}

	@Test
	public void testHillClimbingEpsDynamic() throws Exception {
		runProtocolEVS(SearchType.HC_EPS_DYNAMIC, false);
	}

	@Test
	public void testHillClimbingEpsLex() throws Exception {
		runProtocolEVS(SearchType.HC_EPS_LEX, false);
	}

	@Test
	public void testHillClimbingWeighted() throws Exception {
		runProtocolEVS(SearchType.HC_WEIGHTED, false);
	}

	@Test
	public void testSimulAnnealEps() throws Exception {
		runProtocolEVS(SearchType.SA_EPS, false);
	}

	@Test
	public void testSimulAnnealEpsFlexi() throws Exception {
		runProtocolEVS(SearchType.SA_EPS_FLEX, false);
	}

	@Test
	public void testSimulAnnealEpsFlexiIMDB() throws Exception {
		String[] origUrls = new String[] { "datasets/imdb/imdb_200k.csv",
				"datasets/imdb/imdb_400k.csv", "datasets/imdb/imdb_600k.csv",
				"datasets/imdb/imdb_800k.csv", "datasets/imdb/imdb_1m.csv",
				"datasets/imdb/imdb_1.2m.csv", };
		String[] origNames = new String[] { "imdb_200k", "imdb_400k",
				"imdb_600k", "imdb_800k", "imdb_1m", "imdb_1.2m", };

		for (int i = 0; i < origUrls.length; i++) {

			Config.imdbOrigFileUrl = origUrls[i];
			Config.imdbOrigFileName = origNames[i];
			origUrl = Config.imdbOrigFileUrl;
			origName = Config.imdbOrigFileName;

			logger.log(ProdLevel.PROD, "IMDB URL : " + origUrl);

			runProtocolEVS(SearchType.SA_EPS_FLEX, false);
		}

	}

	@Test
	public void testSimulAnnealEpsDynamic() throws Exception {
		runProtocolEVS(SearchType.SA_EPS_DYNAMIC, false);
	}

	@Test
	public void testSimulAnnealEpsLex() throws Exception {
		runProtocolEVS(SearchType.SA_EPS_LEX, false);
	}

	@Test
	public void testSimulAnnealEpsLexIMDB() throws Exception {
		String[] origUrls = new String[] { "datasets/imdb/imdb_200k.csv",
				"datasets/imdb/imdb_400k.csv", "datasets/imdb/imdb_600k.csv",
				"datasets/imdb/imdb_800k.csv", "datasets/imdb/imdb_1m.csv",
				"datasets/imdb/imdb_1.2m.csv", };
		String[] origNames = new String[] { "imdb_200k", "imdb_400k",
				"imdb_600k", "imdb_800k", "imdb_1m", "imdb_1.2m", };

		for (int i = 0; i < origUrls.length; i++) {

			Config.imdbOrigFileUrl = origUrls[i];
			Config.imdbOrigFileName = origNames[i];
			origUrl = Config.imdbOrigFileUrl;
			origName = Config.imdbOrigFileName;

			logger.log(ProdLevel.PROD, "IMDB URL : " + origUrl);

			runProtocolEVS(SearchType.SA_EPS_LEX, false);
		}

	}

	@Test
	public void testSimulAnnealWeighted() throws Exception {
		runProtocolEVS(SearchType.SA_WEIGHTED, false);
	}

	/**
	 * Quick and dirty method.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSimulAnnealWeightedIMDB() throws Exception {
		String[] origUrls = new String[] { "datasets/imdb/imdb_200k.csv",
				"datasets/imdb/imdb_400k.csv", "datasets/imdb/imdb_600k.csv",
				"datasets/imdb/imdb_800k.csv", "datasets/imdb/imdb_1m.csv",
				"datasets/imdb/imdb_1.2m.csv", };
		String[] origNames = new String[] { "imdb_200k", "imdb_400k",
				"imdb_600k", "imdb_800k", "imdb_1m", "imdb_1.2m", };

		for (int i = 0; i < origUrls.length; i++) {

			Config.imdbOrigFileUrl = origUrls[i];
			Config.imdbOrigFileName = origNames[i];
			origUrl = Config.imdbOrigFileUrl;
			origName = Config.imdbOrigFileName;

			logger.log(ProdLevel.PROD, "IMDB URL : " + origUrl);

			runProtocolEVS(SearchType.SA_WEIGHTED, false);
		}

	}

	/**
	 * Quick and dirty method.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSimulAnnealEpsDynamicIMDB() throws Exception {
		String[] origUrls = new String[] { "datasets/imdb/imdb_200k.csv",
				"datasets/imdb/imdb_400k.csv", "datasets/imdb/imdb_600k.csv",
				"datasets/imdb/imdb_800k.csv", "datasets/imdb/imdb_1m.csv",
				"datasets/imdb/imdb_1.2m.csv", };
		String[] origNames = new String[] { "imdb_200k", "imdb_400k",
				"imdb_600k", "imdb_800k", "imdb_1m", "imdb_1.2m", };

		for (int i = 0; i < origUrls.length; i++) {

			Config.imdbOrigFileUrl = origUrls[i];
			Config.imdbOrigFileName = origNames[i];
			origUrl = Config.imdbOrigFileUrl;
			origName = Config.imdbOrigFileName;

			logger.log(ProdLevel.PROD, "IMDB URL : " + origUrl);

			runProtocolEVS(SearchType.SA_EPS_DYNAMIC, false);
		}

	}

	/**
	 * Use this if you want to consider all repairs for all patterns together.
	 * Will result in better global repairs c.f. repairing pattern by pattern
	 * (where ordering matters).
	 * 
	 * @param subsetViols
	 * @return
	 */
	private List<Set<Long>> combineViolSubsets(List<Set<Long>> subsetViols) {
		List<Set<Long>> tot = new ArrayList<>();
		Set<Long> combined = new HashSet<>();

		for (Set<Long> sub : subsetViols) {
			combined.addAll(sub);
		}

		tot.add(combined);
		return tot;
	}

	private Pair<Objective, Set<Objective>> constructEpsDynamicObjective(
			Constraint constraint, InfoContentTable table) {

		Objective pvtFn = new PrivacyObjective(0d, 0d, false, constraint, table);
		Objective cleanFn = new CustomCleaningObjective(
				Config.EPSILON_PREV_CLEANING, 0d, false, constraint, table);
		Objective changesFn = new ChangesObjective(Config.EPSILON_PREV_SIZE,
				0d, false, constraint, table);
		Set<Objective> constraintFns = new HashSet<>();
		constraintFns.add(cleanFn);
		constraintFns.add(changesFn);

		logger.log(ProdLevel.PROD, "\nMinimizing objective : " + pvtFn);
		logger.log(ProdLevel.PROD, "\nSubject to : " + constraintFns);

		Pair<Objective, Set<Objective>> p = new Pair<>();
		p.setO1(pvtFn);
		p.setO2(constraintFns);

		return p;
	}

	public double calcMaxInd(Constraint constraint, List<Record> records) {
		if (records == null || records.isEmpty() || constraint == null)
			return 0d;

		List<String> cols = constraint.getColsInConstraint();

		if (cols == null || cols.isEmpty())
			return 0;

		Set<String> patterns = new HashSet<>();

		for (Record record : records) {
			String key = record.getRecordStr(cols);
			patterns.add(key);
		}

		return Math.log(patterns.size()) / Math.log(2d);
		// Below is the theoretical max:
		// return Math.log(records.size()) / Math.log(2d);
	}

	private List<Objective> constructEpsLexObjective(Constraint constraint,
			InfoContentTable table) {

		Objective pvtFn = new PrivacyObjective(Config.EPSILON_LEX_PVT, 0d,
				true, constraint, table);
		Objective cleanFn = new CustomCleaningObjective(
				Config.EPSILON_LEX_CLEANING, 0d, true, constraint, table);
		Objective changesFn = new ChangesObjective(0d, 0d, true, constraint,
				table);

		List<Objective> fns = new ArrayList<>();
		fns.add(pvtFn);
		fns.add(cleanFn);
		fns.add(changesFn);

		logger.log(ProdLevel.PROD, "\nMinimizing objective : " + fns);

		return fns;
	}

	private Pair<Objective, Set<Objective>> constructEpsObjective(
			Constraint constraint, InfoContentTable table) {

		Objective pvtFn = new PrivacyObjective(0d, 0d, true, constraint, table);
		Objective cleanFn = new CustomCleaningObjective(
				Config.EPSILON_CLEANING, 0d, true, constraint, table);
		Objective changesFn = new ChangesObjective(Config.EPSILON_SIZE, 0d,
				true, constraint, table);
		Set<Objective> constraintFns = new HashSet<>();
		constraintFns.add(cleanFn);
		constraintFns.add(changesFn);

		logger.log(ProdLevel.PROD, "\nMinimizing objective : " + pvtFn);
		logger.log(ProdLevel.PROD, "\nSubject to : " + constraintFns);

		Pair<Objective, Set<Objective>> p = new Pair<>();
		p.setO1(pvtFn);
		p.setO2(constraintFns);

		return p;
	}

	private Pair<Objective, Set<Objective>> constructEpsFlexibleObjective(
			Constraint constraint, InfoContentTable table) {

		Objective pvtFn = new PrivacyObjective(0d, 0d, true, constraint, table);
		Objective cleanFn = new CustomCleaningObjective(
				Config.EPSILON_FLEX_CLEANING, 0d, true, constraint, table);
		Objective changesFn = new ChangesObjective(Config.EPSILON_FLEX_SIZE,
				0d, true, constraint, table);
		Set<Objective> constraintFns = new HashSet<>();
		constraintFns.add(cleanFn);
		constraintFns.add(changesFn);

		logger.log(ProdLevel.PROD, "\nMinimizing objective : " + pvtFn);
		logger.log(ProdLevel.PROD, "\nSubject to : " + constraintFns);

		Pair<Objective, Set<Objective>> p = new Pair<>();
		p.setO1(pvtFn);
		p.setO2(constraintFns);

		return p;
	}

	private List<Objective> constructWeightedObjective(Constraint constraint,
			InfoContentTable table) {
		List<Objective> weightedFns = new ArrayList<>();
		Objective pvtFn = new PrivacyObjective(0d, Config.ALPHA_PVT, true,
				constraint, table);
		Objective indFn = new CustomCleaningObjective(0d, Config.BETA_IND,
				true, constraint, table);
		Objective changesFn = new ChangesObjective(0d, Config.GAMMA_SIZE, true,
				constraint, table);
		weightedFns.add(pvtFn);
		weightedFns.add(indFn);
		weightedFns.add(changesFn);

		logger.log(ProdLevel.PROD, "\nMinimizing objective : " + weightedFns);

		return weightedFns;
	}

	private void runProtocolEVS(SearchType searchType, boolean shdReturnInit)
			throws Exception {
		for (int errPerc = 0; errPerc < Config.CONSQ_ERR_INJECT.length; errPerc++) {
			reloadConfigs(Config.CONSQ_ERR_INJECT[errPerc]);
			loadDatsets(Config.CONSQ_ERR_INJECT[errPerc]);

			long start = System.nanoTime();

			List<ErrorMetadata> emds = datasetService.loadErrMetadata(
					tgtErrMetaUrl, datasetSeparator, datasetQuoteChar);

			Map<Long, ErrorMetadata> tidToemd = new HashMap<>();

			int numErr = 0;

			for (ErrorMetadata emd : emds) {
				numErr += emd.getErrorsColsToVal().size();
				tidToemd.put(emd.getTid(), emd);
			}

			Metrics met = new Metrics();
			Set<Long> correctedIds = new HashSet<>();
			Set<Long> incorrectIds = new HashSet<>();
			Map<Constraint, Map<Long, Match>> constraintToMatches = new HashMap<>();

			for (int i = 0; i < constraints.size(); i++) {
				Constraint constraint = constraints.get(i);
				InfoContentTable table = m.calcInfoContentTable(constraint);
				Search search = getSearch(searchType, constraint, table,
						errPerc);
				Map<Long, Match> tidToMatch = runProtocolEVS(search,
						Config.shdPartitionViols, constraint, table, tidToemd,
						numErr, met, correctedIds, incorrectIds, shdReturnInit);
				constraintToMatches.put(constraint, tidToMatch);
			}

			printStats(constraintToMatches, met, correctedIds, incorrectIds,
					tidToemd, searchType);

			long end = System.nanoTime();

			logger.log(
					ProdLevel.PROD,
					"time taken (sec) : "
							+ TimeUnit.SECONDS.convert(end - start,
									TimeUnit.NANOSECONDS));
		}
	}

	/**
	 * Prints precision, recall, f1 and also the ids of the records which were
	 * repaired and not repaired.
	 * 
	 * @param constraintToMatches
	 * @param met
	 * @param correctedIds
	 * @param incorrectIds
	 * @param tidToemd
	 * @param searchType
	 */
	private void printStats(
			Map<Constraint, Map<Long, Match>> constraintToMatches, Metrics met,
			Set<Long> correctedIds, Set<Long> incorrectIds,
			Map<Long, ErrorMetadata> tidToemd, SearchType searchType) {

		Set<Long> a = new HashSet<>(correctedIds);
		a.addAll(incorrectIds);

		Set<Long> b = new HashSet<>(tidToemd.keySet());
		Set<Long> b2 = new HashSet<>(tidToemd.keySet());

		b.retainAll(a);
		b2.removeAll(b);

		logger.log(ProdLevel.PROD, "Tids (injected errors) repaired : " + b);
		logger.log(ProdLevel.PROD, "Tids (injected errors) not touched : " + b2);

		printEmdExplanation(b2, tidToemd, constraintToMatches);

		int numCorrectRepairs = met.getNumCorrectRepairs();
		int numTotalRepairs = met.getNumTotalRepairs();
		int numErrors = met.getNumErrors();
		double precision = Stats.precision(numCorrectRepairs, numTotalRepairs);
		double recall = Stats.recall(numCorrectRepairs, numErrors);
		double f1 = Stats.f1(numCorrectRepairs, numTotalRepairs, numErrors);

		logger.log(ProdLevel.PROD, searchType.name() + "[numCorrectRepairs : "
				+ numCorrectRepairs + ", numTotalRepairs : " + numTotalRepairs
				+ ", numErrors : " + numErrors + "]");
		logger.log(ProdLevel.PROD, searchType.name() + "[precision : "
				+ precision + ", recall : " + recall + ", f1 : " + f1 + "]");
	}

	private void printEmdExplanation(Set<Long> untouched,
			Map<Long, ErrorMetadata> tidToemd,
			Map<Constraint, Map<Long, Match>> constraintToMatches) {
		logger.log(ProdLevel.PROD, "Explanation for tids not touched : ");

		int numEmdMatches = 0;

		for (long unt : untouched) {
			ErrorMetadata emd = tidToemd.get(unt);
			boolean wasEmdMatched = false;
			if (emd.getTid() == 17859) {
				System.out.println("fd");
			}
			for (Map.Entry<Constraint, Map<Long, Match>> entry : constraintToMatches
					.entrySet()) {
				Set<String> cols = new HashSet<>(entry.getKey()
						.getColsInConstraint());
				Map<Long, Match> tidToMatch = entry.getValue();
				Match m = tidToMatch.get(emd.getTid());

				if (m != null) {
					Set<String> errcols = emd.getErrorsColsToVal().keySet();
					for (String errCol : errcols) {
						if (cols.contains(errCol)) {
							wasEmdMatched = true;
							break;
						}
					}

				}

				if (wasEmdMatched) {
					break;
				}
			}

			if (wasEmdMatched) {
				numEmdMatches++;
			} else {
				logger.log(ProdLevel.PROD, "Not matched emd : " + emd.getTid()
						+ ", wrt cols : " + emd.getErrorsColsToVal().keySet());
			}
		}

		logger.log(ProdLevel.PROD, "NO MATCHES FOR "
				+ (untouched.size() - numEmdMatches) + " INJECTED ERR!");
	}

	private Search getSearch(SearchType type, Constraint constraint,
			InfoContentTable table, double errPerc) {
		Search search = null;

		// Increase number of iterations for higher error percentage because the
		// size of each violation chunk will be larger.
		double startTemp = Config.START_TEMP * (double) (1d + errPerc);

		logger.log(ProdLevel.PROD, "\n-----------------------------------");

		if (type == SearchType.HC_WEIGHTED) {
			logger.log(ProdLevel.PROD,
					"EXPERIMENT (weighted) : WEIGHTED OBJECTIVE FN, HILL CLIMBING");

			search = new HillClimbingWeighted(constructWeightedObjective(
					constraint, table), Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		} else if (type == SearchType.HC_EPS) {
			logger.log(ProdLevel.PROD,
					"EXPERIMENT (eps) : EPSILON-CONSTRAINT, MIN PRIVACY, "
							+ "IND AND SIZE AS CONSTRAINTS SIMULATED ANNEALING");

			Pair<Objective, Set<Objective>> obj = constructEpsObjective(
					constraint, table);
			search = new HillClimbingEps(obj.getO1(), obj.getO2(),
					Config.INIT_STRATEGY, Config.IND_NORM_STRAT);
		} else if (type == SearchType.HC_EPS_LEX) {
			logger.log(ProdLevel.PROD,
					"EXPERIMENT (eps lex) : EPSILON-HIERARCHICAL, MIN PRIVACY, "
							+ "THEN IND, THEN SIZE, HILL CLIMBING");

			search = new HillClimbingEpsLex(constructEpsLexObjective(
					constraint, table), Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		} else if (type == SearchType.HC_EPS_DYNAMIC) {
			logger.log(ProdLevel.PROD,
					"EXPERIMENT (eps dynamic) : EPSILON-DYNAMIC, MIN PRIVACY, THEN IND, "
							+ "THEN SIZE, HILL CLIMBING");

			Pair<Objective, Set<Objective>> obj = constructEpsDynamicObjective(
					constraint, table);
			search = new HillClimbingEpsDynamic(obj.getO1(), obj.getO2(),
					Config.INIT_STRATEGY, Config.IND_NORM_STRAT);
		} else if (type == SearchType.SA_WEIGHTED) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (weighted) : WEIGHTED OBJECTIVE FN, SIMULATED ANNEALING, START TEMP("
							+ startTemp
							+ "), END TEMP("
							+ Config.FINAL_TEMP
							+ "), ALPHA(" + Config.ALPHA_TEMP + ")");

			search = new SimulAnnealWeighted(constructWeightedObjective(
					constraint, table), startTemp, Config.FINAL_TEMP,
					Config.ALPHA_TEMP, Config.BEST_ENERGY,
					Config.INIT_STRATEGY, Config.IND_NORM_STRAT);

		} else if (type == SearchType.SA_EPS_DYNAMIC) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (eps dynamic) : EPSILON-DYNAMIC, MINIMIZE PRIVACY, SUBJECT TO DYNAMIC CONSTRAINT ON"
							+ " IND AND DYNAMIC CONSTRAINT ON CHANGES FN, SIMULATED ANNEALING, START TEMP("
							+ startTemp
							+ "), END TEMP("
							+ Config.FINAL_TEMP
							+ ")");

			Pair<Objective, Set<Objective>> obj = constructEpsDynamicObjective(
					constraint, table);
			search = new SimulAnnealEpsDynamic(obj.getO1(), obj.getO2(),
					startTemp, Config.FINAL_TEMP, Config.ALPHA_TEMP,
					Config.BEST_ENERGY, Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		} else if (type == SearchType.SA_EPS_LEX) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (eps lex) : EPSILON-HIERARCHICAL, MINIMIZE PRIVACY, THEN IND (SUBJECT TO CONSTRAINT ON PVT), THEN CHANGES (SUBJECT TO CONSTRAINT ON PVT AND IND), SIMULATED ANNEALING, START TEMP("
							+ startTemp
							+ "), END TEMP("
							+ Config.FINAL_TEMP
							+ ")");

			search = new SimulAnnealEpsLex(constructEpsLexObjective(constraint,
					table), startTemp, Config.FINAL_TEMP, Config.ALPHA_TEMP,
					Config.BEST_ENERGY, Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		} else if (type == SearchType.SA_EPS) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (eps) : EPSILON-CONSTRAINT, MINIMIZE PRIVACY SUBJECT TO CONSTRAINTS ON IND AND CHANGES FN, SIMULATED ANNEALING, START TEMP("
							+ startTemp
							+ "), END TEMP("
							+ Config.FINAL_TEMP
							+ ")");

			Pair<Objective, Set<Objective>> obj = constructEpsObjective(
					constraint, table);
			search = new SimulAnnealEps(obj.getO1(), obj.getO2(), startTemp,
					Config.FINAL_TEMP, Config.ALPHA_TEMP, Config.BEST_ENERGY,
					Config.INIT_STRATEGY, Config.IND_NORM_STRAT);
		} else if (type == SearchType.SA_EPS_FLEX) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (epsflex) : EPSILON-CONSTRAINT, MINIMIZE PRIVACY SUBJECT TO CONSTRAINTS ON IND AND CHANGES FN, SIMULATED ANNEALING, START TEMP("
							+ startTemp
							+ "), END TEMP("
							+ Config.FINAL_TEMP
							+ ")");

			Pair<Objective, Set<Objective>> obj = constructEpsFlexibleObjective(
					constraint, table);
			search = new SimulAnnealEpsFlexible(obj.getO1(), obj.getO2(),
					startTemp, Config.FINAL_TEMP, Config.ALPHA_TEMP,
					Config.BEST_ENERGY, Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		}

		logger.log(ProdLevel.PROD, "\nConstraint : " + constraint);
		logger.log(ProdLevel.PROD, "\n-----------------------------------");

		return search;
	}

	private Map<Long, Match> runProtocolEVS(Search search,
			boolean shouldPartitionViols, Constraint constraint,
			InfoContentTable table, Map<Long, ErrorMetadata> tidToemd,
			int numErr, Metrics met, Set<Long> correctedIds,
			Set<Long> incorrectIds, boolean shdReturnInit) {
		List<String> consequent = constraint.getConsequentCols();
		List<String> antecedents = constraint.getAntecedentCols();

		logger.log(
				ProdLevel.PROD,
				"\nInitial InD : "
						+ Stats.ind(constraint, tgtDataset.getRecords()));

		logger.log(ProdLevel.PROD, "Matching similarity : " + simThreshold);

		if (table.getData().length < 100)
			logger.log(ProdLevel.PROD, "\n\nInfo content table :\n\n" + table);

		logger.log(ProdLevel.PROD, "\n-----------------------------------");
		logger.log(ProdLevel.PROD, "Grouping by constraint " + constraint);
		logger.log(ProdLevel.PROD, "-----------------------------------\n");

		// Step 1 : Target finds all violations.
		Violations allViols = t.calcViolations(tgtDataset.getRecords(),
				constraint);
		if (allViols == null || allViols.getViolMap() == null
				|| allViols.getViolMap().isEmpty())
			return null;

		int numChunks = allViols.getViolMap().keySet().size();
		logger.log(ProdLevel.PROD, "Num viols : "
				+ allViols.getViolMap().size() + ", Num viol chunks : "
				+ numChunks);

		int constraintSize = constraint.getColsInConstraint().size();

		List<Set<Long>> partitionViols = w.subsetViolsBySize(allViols);

		if (!shouldPartitionViols)
			partitionViols = combineViolSubsets(partitionViols);

		List<Record> tgtViols = t.getViolRecords(partitionViols);

		// Step 2 : Third party calculates matches for viols.
		List<Match> tgtMatches = w
				.calcPvtMatches(constraint, tgtViols, mDataset.getRecords(),
						tgtDataset.getName(), mDataset.getName());

		if (tgtMatches == null)
			return null;

		Map<Long, Match> tidToMatch = new HashMap<>();

		for (Match m : tgtMatches) {
			if (!(m == null || m.getBestMatches() == null || m.getBestMatches()
					.isEmpty())) {
				tidToMatch.put(m.getOriginalrId(), m);
			}
		}

		if (!Config.shdPartitionViols)
			logger.log(ProdLevel.PROD,
					"\n\nCombining all chunks. Only 1 simulated annealing expt will be performed.");

		for (int i = 0; i < partitionViols.size(); i++) {
			Set<Long> viols = partitionViols.get(i);
			logger.log(ProdLevel.PROD,
					"\n\n-----------------------------------");
			logger.log(ProdLevel.PROD, "Fixing violations [chunk num = "
					+ (i + 1) + " / " + numChunks + ", size = " + viols.size()
					+ "] : \n" + viols);
			// logger.log(
			// ProdLevel.PROD,
			// "Fixing violations [chunk num = " + (i + 1) + " / "
			// + numChunks + ", size = " + viols.size() + "] : \n"
			// + viols + "\n\n"
			// + prettyPrintViols(viols, antecedents, consequent));
			logger.log(ProdLevel.PROD, "-----------------------------------");

			List<Match> partitionMatches = new ArrayList<>();

			for (long v : viols) {
				if (tidToMatch.containsKey(v)) {
					partitionMatches.add(tidToMatch.get(v));
				}
			}

			logger.log(ProdLevel.PROD, "\nMatches for violations ["
					+ constraint + "] : \n" + partitionMatches);

			if (partitionMatches == null || partitionMatches.isEmpty()) {
				continue;
			}

			// Step 3 : Third party calculates recommendations.
			Set<Candidate> solns = w.calcOptimalSolns(constraint,
					partitionMatches, search, table, shdReturnInit);

			List<Recommendation> soln = t
					.selectMostCorrectSoln(solns, tidToemd);

			// Step 4 : Apply recommendation.
			t.applyRecommendationSet(soln);

			// Step 5 : Replace the info content of the revealed values with 0.
			m.removeInfoContentUsingCandidate(table, soln);

			gatherStats(constraintSize, soln, tidToemd, partitionViols, met,
					numErr, correctedIds, incorrectIds);
		}

		return tidToMatch;

	}

	private String prettyPrintViols(Set<Long> recIds, List<String> antecedents,
			List<String> consequent) {
		Multimap<String, Long> conseqToRecs = HashMultimap.create();

		for (long recId : recIds) {
			Record r = tgtDataset.getRecord(recId);
			conseqToRecs.put(r.getRecordStr(consequent, ","), recId);
		}

		StringBuilder sb = new StringBuilder();

		String cAnt = tgtDataset.getRecord(recIds.iterator().next())
				.getRecordStr(antecedents, ",");
		sb.append("Common antecedents : " + cAnt + "\n\n");

		sb.append("Differing consequent : \n");
		for (String consq : conseqToRecs.keySet()) {
			sb.append(conseqToRecs.get(consq) + " : " + consq + "\n");
		}

		return sb.toString();
	}

	private void gatherStats(int constraintSize, List<Recommendation> recs,
			Map<Long, ErrorMetadata> tidToemd, List<Set<Long>> partitionViols,
			Metrics met, int numErrors, Set<Long> correctedIds,
			Set<Long> incorrectIds) {
		if (recs == null || recs.isEmpty())
			return;

		int numCorrectRepairs = 0;
		int numTotalRepairs = 0;

		for (Recommendation rec : recs) {
			if (rec != null) {
				if (tidToemd.containsKey(rec.gettRid())) {

					ErrorMetadata err = tidToemd.get(rec.gettRid());

					if (err.getOrigColsToVal().containsKey(rec.getCol())) {
						String correct = err.getOrigColsToVal()
								.get(rec.getCol()).trim();
						String repaired = rec.getVal().trim();

						if (correct.equals(repaired)) {
							numCorrectRepairs++;
							correctedIds.add(rec.gettRid());
						} else {
							incorrectIds.add(rec.gettRid());
						}
					}
				}

				numTotalRepairs++;
			}
		}

		// Stays the same.
		met.setNumErrors(numErrors);
		met.setNumCorrectRepairs(met.getNumCorrectRepairs() + numCorrectRepairs);
		met.setNumTotalRepairs(met.getNumTotalRepairs() + numTotalRepairs);
	}

	// CLIENT CLASSES (target, master and third party):
	// All the method calls in these classes are chained to the service methods
	// and we can actually remove the client classes and directly call the
	// service methods instead. However, we want to show that the clients follow
	// the protocol, so we create these classes for them.
	public class Master {

		public InfoContentTable calcInfoContentTable(Constraint constraint) {
			return datasetService.calcInfoContentTable(constraint, mDataset);
		}

		public void removeInfoContentUsingCandidate(InfoContentTable table,
				List<Recommendation> soln) {
			datasetService.removeInfoContentUsingCandidate(table, mDataset,
					soln);
		}
	}

	public class Target {

		public void applyManyRecommendationSets(List<Candidate> multiRecs) {
			if (multiRecs == null)
				return;

			for (Candidate recs : multiRecs) {
				List<Recommendation> trimmed = trimExactMatches(recs);
				applyRecommendationSet(trimmed);
			}

			if (tgtDataset.getRecords().size() < 100) {
				logger.log(ProdLevel.PROD, "\nFixed target instance : \n");

				List<Record> fixedTgtRecords = tgtDataset.getRecords();

				for (Record rep : fixedTgtRecords) {
					logger.log(ProdLevel.PROD, rep.prettyPrintRecord(null));
				}

				logger.log(ProdLevel.PROD, "");
			}
		}

		public List<Recommendation> selectMostCorrectSoln(Set<Candidate> solns,
				Map<Long, ErrorMetadata> tidToemd) {
			// TODO
			if (solns == null || solns.isEmpty()) {
				logger.log(ProdLevel.PROD, "\nOptimal solns : null");
				return null;
			}

			logger.log(ProdLevel.PROD, "\nOptimal solns : ");
			List<Recommendation> bestSoln = null;
			int largestCorrectRepairs = Integer.MIN_VALUE;
			int i = 1;
			for (Candidate soln : solns) {
				logger.log(ProdLevel.PROD, "\nSoln #" + i);
				logger.log(ProdLevel.PROD, soln.getRecommendations());
				logger.log(ProdLevel.PROD, soln.getDebugging());

				List<Recommendation> recs = trimExactMatches(soln);
				int numCorrectRepairs = 0;

				for (Recommendation rec : recs) {
					if (rec != null) {
						if (tidToemd.containsKey(rec.gettRid())) {

							ErrorMetadata err = tidToemd.get(rec.gettRid());

							if (err.getOrigColsToVal()
									.containsKey(rec.getCol())) {
								String correct = err.getOrigColsToVal()
										.get(rec.getCol()).trim();
								String repaired = rec.getVal().trim();

								if (correct.equals(repaired)) {
									numCorrectRepairs++;
								}
							}
						}
					}
				}

				if (numCorrectRepairs > largestCorrectRepairs) {
					bestSoln = recs;
					largestCorrectRepairs = numCorrectRepairs;
				}

				i++;

				if (i == 20) {
					logger.log(ProdLevel.PROD,
							"Too many solns, not evaluating everything...");
					break;
				}
			}

			return bestSoln;
		}

		public List<Record> getViolRecords(List<Set<Long>> partitionViols) {
			List<Record> records = new ArrayList<>();

			for (Set<Long> viols : partitionViols) {
				for (long v : viols) {
					records.add(tgtDataset.getRecord(v));
				}
			}

			return records;
		}

		private void applyRecommendationSet(List<Recommendation> recs) {
			logger.log(ProdLevel.PROD, "\n-----------------------------------");
			logger.log(ProdLevel.PROD, "Applying recommendations");
			logger.log(ProdLevel.PROD, "-----------------------------------");

			if (recs == null)
				return;

			logger.log(ProdLevel.PROD, "\n\nSelected soln : " + recs);

			tgtDataset.applyRecommendationSet(recs);

		}

		public Violations calcViolations(List<Record> records,
				Constraint constraint) {
			Violations allViols = repairService.calcViolations(records,
					constraint);

			return allViols;
		}

		/**
		 * Target would obviously like the largest soln.
		 * 
		 * @param solns
		 * @return
		 */
		private List<Recommendation> selectLargestSoln(Set<Candidate> solns) {
			if (solns == null || solns.isEmpty()) {
				logger.log(ProdLevel.PROD, "\nOptimal solns : null");
				return null;
			}

			logger.log(ProdLevel.PROD, "\nOptimal solns : ");
			List<Recommendation> largestSoln = null;
			int largestSolnSize = Integer.MIN_VALUE;
			int i = 1;
			for (Candidate soln : solns) {
				logger.log(ProdLevel.PROD, "\nSoln #" + i);
				logger.log(ProdLevel.PROD, soln.getRecommendations());
				logger.log(ProdLevel.PROD, soln.getDebugging());

				List<Recommendation> recs = trimExactMatches(soln);
				int solnSize = recs.size();

				if (solnSize > largestSolnSize) {
					largestSoln = recs;
					largestSolnSize = solnSize;
				}
				i++;

				if (i == 20) {
					logger.log(ProdLevel.PROD,
							"Too many solns, not evaluating everything...");
					break;
				}
			}

			return largestSoln;
		}

		/**
		 * Target would obviously like the largest soln.
		 * 
		 * @param solns
		 * @return
		 */
		private Candidate selectLargestSolnCandidate(Set<Candidate> solns) {
			if (solns == null || solns.isEmpty()) {
				logger.log(ProdLevel.PROD, "\nOptimal solns : null");
				return null;
			}

			logger.log(ProdLevel.PROD, "\nOptimal solns : ");
			Candidate largestCandiate = null;
			int largestSolnSize = Integer.MIN_VALUE;
			int i = 1;
			for (Candidate soln : solns) {
				logger.log(ProdLevel.PROD, "\nSoln #" + i);
				logger.log(ProdLevel.PROD, soln.getRecommendations());
				logger.log(ProdLevel.PROD, soln.getDebugging());

				List<Recommendation> recs = trimExactMatches(soln);
				int solnSize = recs.size();

				if (solnSize > largestSolnSize) {
					largestSolnSize = solnSize;
					largestCandiate = soln;
				}
				i++;

				if (i == 20) {
					logger.log(ProdLevel.PROD,
							"Too many solns, not evaluating everything...");
					break;
				}
			}

			return largestCandiate;
		}

		private List<Recommendation> trimExactMatches(Candidate soln) {
			List<Recommendation> trimmed = new ArrayList<>();

			for (Recommendation r : soln.getRecommendations()) {
				if (r != null) {
					long tid = r.gettRid();
					Record t = tgtDataset.getRecord(tid);
					String tVal = t.getColsToVal().get(r.getCol());

					if (!tVal.equals(r.getVal())) {
						trimmed.add(r);
					}

				}
			}

			return trimmed;
		}
	}

	public class ThirdParty {

		public List<Match> calcPvtMatches(Constraint constraint,
				List<Record> tgtRecords, List<Record> mRecords,
				String tgtFileName, String mFileName) {
			List<Match> tgtMatches = matchingService
					.applyPvtApproxDataMatching(constraint, tgtRecords,
							mRecords, Config.NUM_STRINGS, Config.DIM_REDUCTION,
							simThreshold, Config.SHOULD_APPROX_DIST,
							Config.SHOULD_AVERAGE, tgtFileName, mFileName);

			return tgtMatches;
		}

		public List<Match> calcMatches(Constraint constraint,
				List<Record> tgtRecords, List<Record> mRecords,
				String tgtFileName, String mFileName) {
			List<Match> tgtMatches = matchingService.applyApproxDataMatching(
					constraint, tgtRecords, mRecords, simThreshold,
					tgtFileName, mFileName);

			return tgtMatches;
		}

		public Set<Candidate> calcOptimalSolns(Constraint constraint,
				List<Match> tgtMatches, Search search, InfoContentTable table,
				boolean shdReturnInit) {
			return repairService.calcOptimalSolns(constraint, tgtMatches,
					search, tgtDataset, mDataset, table, shdReturnInit);

		}

		public List<Set<Long>> subsetViolsBySize(Violations viols) {
			return repairService.subsetViolsBySize(viols);
		}

	}

	public void reloadConfigs(float errPerc) {
		String err = (int) (errPerc * 100) + "";
		String ext = Config.ERR_GEN_STRATEGY == ErrorGenStrategy.RANDOM ? "_temd"
				+ err
				: "_temdc" + err;

		tgtErrName = origName + "_te" + err;
		mName = origName + "_m" + err;

		tgtErrUrl = inject(origUrl, "_te" + err);
		mUrl = inject(origUrl, "_m" + err);
		gtUrl = inject(origUrl, "_gt");
		tgtErrMetaUrl = inject(origUrl, ext);
	}

	private String inject(String origFileUrl, String extension) {
		String prefix = origFileUrl.substring(0, origFileUrl.length() - 4);
		return prefix + extension + ".csv";
	}

	@Test
	public void testSimulAnnealEpsLexEVP() throws Exception {
		for (int i = 0; i < Config.EPSILON_VS_PVT_LOSS.length; i++) {
			runProtocolEVP(SearchType.SA_EPS_LEX, Config.EPSILON_VS_PVT_LOSS[i]);
		}
	}

	@Test
	public void testSimulAnnealEpsEVP() throws Exception {
		for (int i = 0; i < Config.EPSILON_VS_PVT_LOSS.length; i++) {
			runProtocolEVP(SearchType.SA_EPS, Config.EPSILON_VS_PVT_LOSS[i]);
		}
	}

	private void runProtocolEVP(SearchType searchType, double epsPercentage)
			throws Exception {
		for (int errPerc = 0; errPerc < Config.CONSQ_ERR_INJECT.length; errPerc++) {
			double totPvtLoss = 0d;

			reloadConfigs(Config.CONSQ_ERR_INJECT[errPerc]);
			loadDatsets(Config.CONSQ_ERR_INJECT[errPerc]);

			long start = System.nanoTime();

			for (int i = 0; i < constraints.size(); i++) {
				Constraint constraint = constraints.get(i);
				InfoContentTable table = m.calcInfoContentTable(constraint);
				List<Objective> fns = constructWeightedObjective(constraint,
						table);
				RandomWalk rwSearch = new RandomWalk(fns,
						Config.RANDOM_WALK_ITERATIONS, Config.INIT_STRATEGY,
						Config.IND_NORM_STRAT);
				Search search = getSearchEVP(searchType, constraint, table,
						errPerc);
				totPvtLoss += runProtocolEVP(search, rwSearch,
						Config.shdPartitionViols, constraint, table,
						epsPercentage);
			}

			logger.log(ProdLevel.PROD, searchType.name() + ", Eps : "
					+ epsPercentage + ", Total pvt loss : " + totPvtLoss + "\n");

			long end = System.nanoTime();

			logger.log(
					ProdLevel.PROD,
					"time taken (sec) : "
							+ TimeUnit.SECONDS.convert(end - start,
									TimeUnit.NANOSECONDS));
		}
	}

	private double runProtocolEVP(Search search, RandomWalk rwSearch,
			boolean shouldPartitionViols, Constraint constraint,
			InfoContentTable table, double epsPercentage) {
		List<String> consequent = constraint.getConsequentCols();
		List<String> antecedents = constraint.getAntecedentCols();

		logger.log(
				ProdLevel.PROD,
				"\nInitial InD : "
						+ Stats.ind(constraint, tgtDataset.getRecords()));

		logger.log(ProdLevel.PROD, "Matching similarity : " + simThreshold);

		if (table.getData().length < 100)
			logger.log(ProdLevel.PROD, "\n\nInfo content table :\n\n" + table);

		logger.log(ProdLevel.PROD, "\n-----------------------------------");
		logger.log(ProdLevel.PROD, "Grouping by constraint " + constraint);
		logger.log(ProdLevel.PROD, "-----------------------------------\n");

		// Step 1 : Target finds all violations.
		Violations allViols = t.calcViolations(tgtDataset.getRecords(),
				constraint);
		if (allViols == null || allViols.getViolMap() == null
				|| allViols.getViolMap().isEmpty())
			return 0d;

		int numChunks = allViols.getViolMap().keySet().size();
		logger.log(ProdLevel.PROD, "Num viols : "
				+ allViols.getViolMap().size() + ", Num viol chunks : "
				+ numChunks);

		int constraintSize = constraint.getColsInConstraint().size();

		List<Set<Long>> partitionViols = w.subsetViolsBySize(allViols);

		if (!shouldPartitionViols)
			partitionViols = combineViolSubsets(partitionViols);

		List<Record> tgtViols = t.getViolRecords(partitionViols);

		// Step 2 : Third party calculates matches for viols.
		List<Match> tgtMatches = w
				.calcMatches(constraint, tgtViols, mDataset.getRecords(),
						tgtDataset.getName(), mDataset.getName());

		if (tgtMatches == null)
			return 0d;

		Map<Long, Match> tidToMatch = new HashMap<>();

		for (Match m : tgtMatches) {
			if (!(m == null || m.getBestMatches() == null || m.getBestMatches()
					.isEmpty())) {
				tidToMatch.put(m.getOriginalrId(), m);
			}
		}

		PrivacyObjective pObj = new PrivacyObjective(0d, 0d, true, constraint,
				table);
		double maxPvt = table.getMaxInfoContent();
		double totPvtLoss = 0d;

		if (!Config.shdPartitionViols)
			logger.log(ProdLevel.PROD,
					"\n\nCombining all chunks. Only 1 simulated annealing expt will be performed.");

		for (int i = 0; i < partitionViols.size(); i++) {
			Set<Long> viols = partitionViols.get(i);
			logger.log(ProdLevel.PROD,
					"\n\n-----------------------------------");
			logger.log(ProdLevel.PROD, "Fixing violations [chunk num = "
					+ (i + 1) + " / " + numChunks + ", size = " + viols.size()
					+ "] : \n" + viols);
			// logger.log(
			// ProdLevel.PROD,
			// "Fixing violations [chunk num = " + (i + 1) + " / "
			// + numChunks + ", size = " + viols.size() + "] : \n"
			// + viols + "\n\n"
			// + prettyPrintViols(viols, antecedents, consequent));
			logger.log(ProdLevel.PROD, "-----------------------------------");

			List<Match> partitionMatches = new ArrayList<>();

			for (long v : viols) {
				if (tidToMatch.containsKey(v)) {
					partitionMatches.add(tidToMatch.get(v));
				}
			}

			logger.log(ProdLevel.PROD, "\nMatches for violations ["
					+ constraint + "] : \n" + partitionMatches);

			if (partitionMatches == null || partitionMatches.isEmpty()) {
				continue;
			}

			// Perform a random walk first to get some stats.
			w.calcOptimalSolns(constraint, partitionMatches, rwSearch, table,
					false);
			Map<Objective, RandomWalkStats> objectiveToStats = rwSearch
					.getObjectiveToStats();

			// Use stats to update the epsilons on the search.
			if (search instanceof SimulAnnealEps) {
				SimulAnnealEps r = (SimulAnnealEps) search;
				Set<Objective> bfns = r.getBoundedObjectives();
				for (Objective bfn : bfns) {
					// Set epsilon on ind only.
					if (bfn instanceof CustomCleaningObjective) {
						RandomWalkStats st = objectiveToStats.get(bfn);
						if (st != null) {
							logger.log(
									ProdLevel.PROD,
									"Updated CustomCleaningObjective epsilon from: "
											+ bfn.getEpsilon()
											+ " to:"
											+ (st.getMax() - (epsPercentage * (st
													.getMax() - st.getMin()))));
							logger.log(ProdLevel.PROD, st);
							bfn.setEpsilon(epsPercentage * st.getMax());
						}
					}
				}
			} else if (search instanceof SimulAnnealEpsLex) {
				SimulAnnealEpsLex r = (SimulAnnealEpsLex) search;
				List<Objective> fns = r.getObjectives();
				for (Objective fn : fns) {
					// Set epsilon on pvt only.
					if (fn instanceof PrivacyObjective) {
						RandomWalkStats st = objectiveToStats.get(fn);
						if (st != null) {
							logger.log(
									ProdLevel.PROD,
									"Updated PrivacyObjective epsilon from: "
											+ fn.getEpsilon()
											+ " to:"
											+ (st.getMax() - (epsPercentage * (st
													.getMax() - st.getMin()))));
							logger.log(ProdLevel.PROD, st);
							fn.setEpsilon(epsPercentage * st.getMax());
						}
					}
				}
			}

			// Step 3 : Third party calculates recommendations.
			Set<Candidate> solns = w.calcOptimalSolns(constraint,
					partitionMatches, search, table, false);

			Candidate soln = t.selectLargestSolnCandidate(solns);

			if (soln == null)
				continue;

			List<Recommendation> solnRecs = soln.getRecommendations();

			// Step 4 : Apply recommendation.
			t.applyRecommendationSet(solnRecs);

			totPvtLoss += pObj.out(soln, tgtDataset, mDataset, maxPvt, 0, 0);

			// Step 5 : Replace the info content of the revealed values with 0.
			m.removeInfoContentUsingCandidate(table, solnRecs);

		}

		return totPvtLoss;
	}

	private Search getSearchEVP(SearchType type, Constraint constraint,
			InfoContentTable table, double errPerc) {

		Search search = null;

		// Increase number of iterations for higher error percentage because the
		// size of each violation chunk will be larger.
		double startTemp = Config.START_TEMP_EVP * (double) (1d + errPerc);

		logger.log(ProdLevel.PROD, "\n-----------------------------------");

		if (type == SearchType.SA_EPS) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (eps) : EPSILON-CONSTRAINT, MINIMIZE PRIVACY SUBJECT TO CONSTRAINTS ON IND AND CHANGES FN, SIMULATED ANNEALING, START TEMP("
							+ startTemp
							+ "), END TEMP("
							+ Config.FINAL_TEMP
							+ ")");

			Pair<Objective, Set<Objective>> obj = constructEpsObjective(
					constraint, table);
			search = new SimulAnnealEps(obj.getO1(), obj.getO2(), startTemp,
					Config.FINAL_TEMP_EVP, Config.ALPHA_TEMP_EVP,
					Config.BEST_ENERGY_EVP, Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		} else if (type == SearchType.SA_EPS_FLEX) {
			logger.log(
					ProdLevel.PROD,
					"EXPERIMENT (epsflex evp) : EPSILON-CONSTRAINT, MINIMIZE "
							+ "PRIVACY SUBJECT TO CONSTRAINTS ON IND AND CHANGES FN, "
							+ "SIMULATED ANNEALING, START TEMP(" + startTemp
							+ "), END TEMP(" + Config.FINAL_TEMP_EVP + ")");

			Pair<Objective, Set<Objective>> obj = constructEpsFlexibleObjectiveEVP(
					constraint, table);
			search = new SimulAnnealEpsFlexible(obj.getO1(), obj.getO2(),
					startTemp, Config.FINAL_TEMP_EVP, Config.ALPHA_TEMP_EVP,
					Config.BEST_ENERGY_EVP, Config.INIT_STRATEGY,
					Config.IND_NORM_STRAT);
		}

		logger.log(ProdLevel.PROD, "\nConstraint : " + constraint);
		logger.log(ProdLevel.PROD, "\n-----------------------------------");

		return search;
	}

	private Pair<Objective, Set<Objective>> constructEpsFlexibleObjectiveEVP(
			Constraint constraint, InfoContentTable table) {

		Objective pvtFn = new PrivacyObjective(0d, 0d, true, constraint, table);
		Objective cleanFn = new CustomCleaningObjective(
				Config.EPSILON_FLEX_CLEANING, 0d, true, constraint, table);
		Objective changesFn = new ChangesObjective(Config.EPSILON_FLEX_SIZE,
				0d, true, constraint, table);
		Set<Objective> constraintFns = new HashSet<>();
		constraintFns.add(cleanFn);
		constraintFns.add(changesFn);

		logger.log(ProdLevel.PROD, "\nMinimizing objective : " + pvtFn);
		logger.log(ProdLevel.PROD, "\nSubject to : " + constraintFns);

		Pair<Objective, Set<Objective>> p = new Pair<>();
		p.setO1(pvtFn);
		p.setO2(constraintFns);

		return p;
	}
}
