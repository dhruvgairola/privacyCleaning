package data.cleaning.core.utils;

import org.apache.log4j.Level;

/**
 * This is a custom {@link org.apache.log4j.Level} for DEBUG logging. Nods to
 * Jaikiran Pai for his example code {@linkplain http
 * ://jaikiran.wordpress.com/2006/07/12/create-your-own-logging-level-in-log4j/}
 * of which this is a total bite. Also, thanks to Roman Hustad for helped me
 * realize I should stop bitching about Log4j not having one of these and make
 * it myself.
 * 
 * If you're getting a "No appenders could be found" error, make sure your
 * log4j.xml or log4j.properties file is on the classpath, because it isn't.
 * 
 * @author Arshan Dabirsiaghi
 * 
 */

public class DebugLevel extends Level {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3145860241567535095L;

	/**
	 * Value of DEBUG level. This value is slightly higher than
	 * {@link org.apache.log4j.Priority#INFO_INT}.
	 */
	public static final int DEBUG_LEVEL_INT = Level.INFO_INT + 2;

	/**
	 * {@link Level} representing my log level
	 */
	public static final Level DEBUG = new DebugLevel(DEBUG_LEVEL_INT,
			"DEBUG", 7);

	private static final String DEBUG_MSG = "DEBUG";

	/**
	 * Default constructor.
	 */
	protected DebugLevel(int arg0, String arg1, int arg2) {
		super(arg0, arg1, arg2);

	}

	/**
	 * Checks whether <code>sArg</code> is "DEBUG" level. If yes then returns
	 * {@link DebugLevel#DEBUG}, else calls
	 * {@link DebugLevel#toLevel(String, Level)} passing it
	 * {@link Level#DEBUG} as the defaultLevel
	 *
	 * @see Level#toLevel(java.lang.String)
	 * @see Level#toLevel(java.lang.String, org.apache.log4j.Level)
	 *
	 */
	public static Level toLevel(String sArg) {
		if (sArg != null && sArg.toUpperCase().equals(DEBUG_MSG)) {
			return DEBUG;
		}
		return (Level) toLevel(sArg, Level.DEBUG);
	}

	/**
	 * Checks whether <code>val</code> is
	 * {@link DebugLevel#DEBUG_LEVEL_INT}. If yes then returns
	 * {@link DebugLevel#DEBUG}, else calls
	 * {@link DebugLevel#toLevel(int, Level)} passing it {@link Level#DEBUG}
	 * as the defaultLevel
	 *
	 * @see Level#toLevel(int)
	 * @see Level#toLevel(int, org.apache.log4j.Level)
	 *
	 */
	public static Level toLevel(int val) {
		if (val == DEBUG_LEVEL_INT) {
			return DEBUG;
		}
		return (Level) toLevel(val, Level.DEBUG);
	}

	/**
	 * Checks whether <code>val</code> is
	 * {@link DebugLevel#DEBUG_LEVEL_INT}. If yes then returns
	 * {@link DebugLevel#DEBUG}, else calls
	 * {@link Level#toLevel(int, org.apache.log4j.Level)}
	 *
	 * @see Level#toLevel(int, org.apache.log4j.Level)
	 */
	public static Level toLevel(int val, Level defaultLevel) {
		if (val == DEBUG_LEVEL_INT) {
			return DEBUG;
		}
		return Level.toLevel(val, defaultLevel);
	}

	/**
	 * Checks whether <code>sArg</code> is "DEBUG" level. If yes then returns
	 * {@link DebugLevel#DEBUG}, else calls
	 * {@link Level#toLevel(java.lang.String, org.apache.log4j.Level)}
	 *
	 * @see Level#toLevel(java.lang.String, org.apache.log4j.Level)
	 */
	public static Level toLevel(String sArg, Level defaultLevel) {
		if (sArg != null && sArg.toUpperCase().equals(DEBUG_MSG)) {
			return DEBUG;
		}
		return Level.toLevel(sArg, defaultLevel);
	}
}