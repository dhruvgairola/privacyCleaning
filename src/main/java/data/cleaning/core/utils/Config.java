package data.cleaning.core.utils;

import java.util.EnumSet;
import java.util.Set;

import data.cleaning.core.service.errgen.impl.ErrorGenStrategy;
import data.cleaning.core.utils.objectives.IndNormStrategy;
import data.cleaning.core.utils.search.InitStrategy;

public class Config {
	// Reproducibility of expts.
	public static final int SEED = 17;

	// When comparing floats, what is the threshold for equality?
	public static final float FLOAT_EQUALIY_EPSILON = 0.00005f;

	// Dataset error generation.
	public static final double PERCENTAGE_CONS_ERRS = 0.10f;
	public static final double PERCENTAGE_ANTS_ERRS = 0.02f;
	// Chunk size is the size of a violation chunk (LHS is the same and RHS is
	// different). Choose wisely bec. for simul annealing, you will need to
	// increase num iterations if chunk is larger.
	public static int ROUGH_CHUNK_SIZE = 100;
	public static final ErrorGenStrategy ERR_GEN_STRATEGY = ErrorGenStrategy.RANDOM;
	// IMDB 50k increasing errors
	// public static final int[] NUM_NON_MATCH = new int[] { 0, 5, 100, 200,
	// 300, -1 };
	// public static final int[] NUM_INCORRECT_MATCHES = new int[] { 0, 5, 30,
	// 60, 120, 200 };
	
//	// Comparative, IMDB 100k, 0.08f err
//	public static final int[] NUM_NON_MATCH = new int[] { -1 };
//	public static final int[] NUM_INCORRECT_MATCHES = new int[] { 0 };

	public static final int[] NUM_NON_MATCH = new int[] { 10, 450, 950, 1800,
			2800, -1 };
	public static final int[] NUM_INCORRECT_MATCHES = new int[] { 1, 350, 550,
			950, 1450, 2180 };
	
	// IMDB increasing tuples
	public static final int[] NUM_NON_MATCH_INC_TUPLES = new int[] { 2, 150,
			260, 630, 860, 1220 };
	public static final int[] NUM_INCORRECT_MATCHES_INC_TUPLES = new int[] { 0,
			30, 90, 200, 350, 520 };
	public static final float BELOW_THRESHOLD_DISTR = 0f;

	// Performance tests.
	public static final int[] P_NUM_NON_MATCH = new int[] { 10, 450, 950, 1800,
			2800, 4000, -1 };
	public static final int[] P_NUM_INCORRECT_MATCHES = new int[] { 1, 350, 550,
			950, 1450, 2180, 3200 };
	// IMDB increasing tuples
	public static final int[] P_NUM_NON_MATCH_INC_TUPLES = new int[] { 2, 150,
			260, 630, 860, 1220, 1800 };
	public static final int[] P_NUM_INCORRECT_MATCHES_INC_TUPLES = new int[] { 0,
			30, 90, 200, 350, 520, 720 };

	// Generic dataset configs.
	public enum Dataset {
		HEALTH, CORA, BOOKS, IMDB, POLLUTION;
	}

	public static final Dataset CURRENT_DATASET = Dataset.IMDB;
	public static final char DATASET_SEPARATOR = ',';
	public static final char DATASET_QUOTE_CHAR = '\'';
	public static final char DATASET_DOUBLE_QUOTE_CHAR = '\"';
	public static final String FILE_CACHE_BASE_URL = "filecache/";

	public static final Float[] CONSQ_ERR_INJECT = new Float[] { 0.08f };
//	 public static final Float[] CONSQ_ERR_INJECT = new Float[] { 0.02f,
//	 0.04f, 0.06f, 0.08f, 0.10f, 0.12f, 0.14f };

	// Loading health
	public static final String HEALTH_ORIG_FILE_NAME = "health";
	public static final String HEALTH_ORIG_FILE_URL = "datasets/health/"
			+ HEALTH_ORIG_FILE_NAME + ".csv";
	public static final String HEALTH_ORIG_FD_URL = "datasets/health/health_fds.csv";
	public static final float HEALTH_SIM_THRESHOLD = 0.97f;
	public static final float[] HEALTH_SIM_THRESHOLDS = new float[] { 0.6f,
			0.7f, 0.8f, 0.9f, 1.0f };

	// Loading cora
	public static final String CORA_ORIG_FILE_NAME = "cora";
	public static final String CORA_ORIG_FILE_URL = "datasets/cora/"
			+ CORA_ORIG_FILE_NAME + ".csv";
	public static final String CORA_ORIG_FD_URL = "datasets/cora/cora_fds.csv";
	public static final float CORA_SIM_THRESHOLD = 0.91f;
	public static final float[] CORA_SIM_THRESHOLDS = new float[] { 0.6f, 0.7f,
			0.8f, 0.9f, 1.0f };

	public static String imdbOrigFileName = "imdb_500k";
	public static String imdbOrigFileUrl = "datasets/imdb/" + imdbOrigFileName
			+ ".csv";
	public static final String IMDB_ORIG_FD_URL = "datasets/imdb/imdb_fds.csv";
	public static final float IMDB_SIM_THRESHOLD = 0.726f;
	public static final float[] IMDB_SIM_THRESHOLDS = new float[] { 0.6f, 0.7f,
			0.8f, 0.9f, 1.0f };

	public static String booksOrigFileName = "books_500k";
	public static String booksOrigFileUrl = "datasets/books/"
			+ booksOrigFileName + ".csv";
	public static final String BOOKS_ORIG_FD_URL = "datasets/books/books_fds.csv";
	public static final float BOOKS_SIM_THRESHOLD = 0.7f;
	public static final float[] BOOKS_SIM_THRESHOLDS = new float[] { 0.6f,
			0.7f, 0.8f, 0.9f, 1.0f };

	public static final String POLLUTION_ORIG_FILE_NAME = "pollution";
	public static final String POLLUTION_ORIG_FILE_URL = "datasets/pollution/"
			+ POLLUTION_ORIG_FILE_NAME + ".csv";
	public static final String POLLUTION_ORIG_FD_URL = "datasets/pollution/pollution_fds.csv";
	public static final float POLLUTION_SIM_THRESHOLD = 0.994f;
	public static final float[] POLLUTION_SIM_THRESHOLDS = new float[] { 0.6f,
			0.7f, 0.8f, 0.9f, 1.0f };

	// Private data matching settings.
	public static final int NUM_STRINGS = 40;
	public static final double DIM_REDUCTION = 0.9d;
	public static final boolean SHOULD_APPROX_DIST = true;
	public static final int TOP_K_MATCHES = 5;
	// This is to toggle the decision rule matching on and off. true = off.
	public static final boolean SHOULD_AVERAGE = true;

	public static final InitStrategy INIT_STRATEGY = InitStrategy.GREEDY_BEST_MATCH;
	public static boolean shdPartitionViols = true;

	// Simulated annealing settings.
	public static final double START_TEMP = 0.0009d;
	public static final double FINAL_TEMP = 0.00005d;
	public static final double ALPHA_TEMP = 0.99d;
	public static final double STEPS_PER_TEMP = 1;
	public static final double BEST_ENERGY = 0.00001d;

	// Simulated annealing settings- epsilon vs pvt loss.
	public static final double START_TEMP_EVP = 0.0009d;
	public static final double FINAL_TEMP_EVP = 0.00005d;
	public static final double ALPHA_TEMP_EVP = 0.998d;
	public static final double STEPS_PER_TEMP_EVP = 1;
	public static final double BEST_ENERGY_EVP = 0.0001d;

	// Weighted scalarization single objective.
	public static final double ALPHA_PVT = 0.10d;
	public static final double BETA_IND = 0.895d;
	public static final double GAMMA_SIZE = 0.005d;

	// Epsilon constraints.
	public static final double EPSILON_PVT = 0.1d;
	public static final double EPSILON_CLEANING = 0.95d;
	public static final double EPSILON_SIZE = 0.6d;

	// Epsilon flex constraints.
	public static final double EPSILON_FLEX_CLEANING = 1.0d;
	public static final double EPSILON_FLEX_SIZE = 1.0d;

	// Epsilon lex constraints.
	public static final double EPSILON_LEX_PVT = 1.0d;
	public static final double EPSILON_LEX_CLEANING = 1.0d;

	// Epsilon dynamic constraints.
	public static final double EPSILON_PREV_PVT = 1.0d;
	public static final double EPSILON_PREV_CLEANING = 1.0d;
	public static final double EPSILON_PREV_SIZE = 1.0d;

	public static final Set<Version> VERSION = EnumSet.of(Version.MEMORY_SAVER,
			Version.REMOVE_EXACT_MATCHES, Version.REMOVE_DUPS_FROM_MATCHES);

	public static final IndNormStrategy IND_NORM_STRAT = IndNormStrategy.INITIAL_IND;

	// File cache settings.
	// What do you want to cache?
	// Adjust this to REFRESH_MATCHES_CACHE if you ever change the current
	// dataset.
	public static final Set<FileCache> FILE_CACHE = EnumSet.of(FileCache.NONE);

	// In-memory cache settings.
	// Used only for optimization purposes.
	// For simul annealing, if the neighbourhood is small, we
	// don't want the algorithm to keep repeating and evaluating the same
	// neighbs again and again.
	public static final int SA_REPEAT_NEIGHB_CACHE_SIZE = 300;
	public static final int SA_REPEAT_NEIGHB_THRESHOLD = 3;

	public static final Double[] EPSILON_VS_PVT_LOSS = new Double[] { 0.84d,
			0.86d, 0.88d, 0.90d, 0.92d, 0.94d };

	// Used for epsilon vs pvt expts to determine the range of objective
	// outputs. The range is used to set the epsilon.
	public static final int RANDOM_WALK_ITERATIONS = 1000;

}
