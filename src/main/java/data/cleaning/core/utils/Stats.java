/**
 */
package data.cleaning.core.utils;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.Record;

public class Stats {

	public static double mutualInfo(List<Record> records, List<String> cols,
			List<String> cols2) {
		List<String> cols12 = new ArrayList<String>(cols);
		cols12.addAll(cols2);

		return entropy(records, cols) + entropy(records, cols2)
				- entropy(records, cols12);
	}

	public static double entropy(List<Record> records, List<String> cols) {
		// TODO: Improve efficiency. Use an index.
		if (cols == null || cols.isEmpty())
			return 0;

		double entropy = 0d;
		double denom = (double) records.size();
		Multiset<String> patternToCount = HashMultiset.create();

		for (Record record : records) {
			patternToCount.add(record.getRecordStr(cols));
		}

		for (String pattern : patternToCount.elementSet()) {
			double num = (double) patternToCount.count(pattern);
			entropy += (num / denom) * (Math.log(denom / num) / Math.log(2d));
		}

		return entropy;
	}

	public static double ind(Constraint c, List<Record> records) {

		List<String> ants = Lists.newArrayList(c.getAntecedent().split(","));
		double antsEntropy = entropy(records, ants);
		List<String> antsAndCons = Lists.newArrayList(c.getAntecedent().split(
				","));
		antsAndCons.addAll(Lists.newArrayList(c.getConsequent().split(",")));
		double antsAndConsEntropy = entropy(records, antsAndCons);
		return antsAndConsEntropy - antsEntropy;
	}

	/**
	 * @param correctReps - num emds which were correct
	 * @param totReps - simul anneal suggestions
	 * @return
	 */
	public static double precision(int correctReps, int totReps) {
		if (totReps == 0)
			return 0d;

		double precision = (double) correctReps / (double) totReps;
		return precision;
	}

	/**
	 * @param correctReps - num emds which were correct
	 * @param numErrs - tot num emds
	 * @return
	 */
	public static double recall(int correctReps, int numErrs) {
		if (numErrs == 0)
			return 0d;

		double recall = (double) correctReps / (double) numErrs;
		return recall;
	}

	public static double f1(int correctReps, int totReps, int totErrs) {
		double precision = precision(correctReps, totReps);
		double recall = recall(correctReps, totErrs);

		if (precision + recall == 0)
			return 0d;

		double f1 = 2 * ((precision * recall) / (precision + recall));
		return f1;
	}
}
