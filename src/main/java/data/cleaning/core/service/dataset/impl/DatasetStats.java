package data.cleaning.core.service.dataset.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import data.cleaning.core.utils.Pair;

/**
 * Maintain some statistics about the dataset which can be used for quickly
 * calculating InD scores.
 * 
 * @author dhruvgairola
 *
 */
public class DatasetStats {
	// Constraint to antecedent patterns.
	private Map<Constraint, Multiset<String>> constraintToAntsP;
	// Constraint to antecedent and consequent patterns.
	private Map<Constraint, Multiset<String>> constraintToAntsConsP;
	// Constraint to antecedent entropy.
	private Map<Constraint, Double> constraintToAntsEntropy;
	// Constraint to antecedent + consequent entropy.
	private Map<Constraint, Double> constraintToAntsAndConsEntropy;
	// Constraint to tid and associated ant string and ants + cons string.
	private Map<Constraint, Map<Long, Pair<String, String>>> constraintTotIdToAntsAndAntsCons;

	public DatasetStats() {
		this.constraintToAntsP = new HashMap<>();
		this.constraintToAntsConsP = new HashMap<>();
		this.constraintToAntsEntropy = new HashMap<>();
		this.constraintToAntsAndConsEntropy = new HashMap<>();
		this.constraintTotIdToAntsAndAntsCons = new HashMap<>();
	}

	public Map<Constraint, Multiset<String>> getConstraintToAntsP() {
		return constraintToAntsP;
	}

	public Multiset<String> getAntPToCount(Constraint constraint) {
		return constraintToAntsP.get(constraint);
	}

	public void setConstraintToAntsP(
			Map<Constraint, Multiset<String>> constraintToAntsP) {
		this.constraintToAntsP = constraintToAntsP;
	}

	public Map<Constraint, Multiset<String>> getConstraintToAntsConsP() {
		return constraintToAntsConsP;
	}

	public Multiset<String> getAntsConsPToCount(Constraint constraint) {
		return constraintToAntsConsP.get(constraint);
	}

	public void setConstraintToAntsConsP(
			Map<Constraint, Multiset<String>> constraintToAntsConsP) {
		this.constraintToAntsConsP = constraintToAntsConsP;
	}

	public Map<Constraint, Double> getConstraintToAntsEntropy() {
		return constraintToAntsEntropy;
	}

	public void setConstraintToAntsEntropy(
			Map<Constraint, Double> constraintToAntsEntropy) {
		this.constraintToAntsEntropy = constraintToAntsEntropy;
	}

	public Map<Constraint, Double> getConstraintToAntsAndConsEntropy() {
		return constraintToAntsAndConsEntropy;
	}

	public void setConstraintToAntsAndConsEntropy(
			Map<Constraint, Double> constraintToAntsAndConsEntropy) {
		this.constraintToAntsAndConsEntropy = constraintToAntsAndConsEntropy;
	}

	public double getAntsEntropy(Constraint constraint) {
		return constraintToAntsEntropy.get(constraint);
	}

	public double getAntsAndConsEntropy(Constraint constraint) {
		return constraintToAntsAndConsEntropy.get(constraint);
	}

	public Map<Long, Pair<String, String>> gettIdToAntsAndAntsCons(
			Constraint constraint) {
		return constraintTotIdToAntsAndAntsCons.get(constraint);
	}

	public Map<Constraint, Map<Long, Pair<String, String>>> getConstraintTotIdToAntsAndAntsCons() {
		return constraintTotIdToAntsAndAntsCons;
	}

	public void setConstraintTotIdToAntsAndAntsCons(
			Map<Constraint, Map<Long, Pair<String, String>>> constraintTotIdToAntsAndAntsCons) {
		this.constraintTotIdToAntsAndAntsCons = constraintTotIdToAntsAndAntsCons;
	}

	public Multiset<String> getAntsConsPToCountCopy(Constraint constraint) {
		return HashMultiset.create(constraintToAntsConsP.get(constraint));
	}

	public Multiset<String> getAntPToCountCopy(Constraint constraint) {
		return HashMultiset.create(constraintToAntsP.get(constraint));
	}

	public Map<Long, Pair<String, String>> gettIdToAntsAndAntsConsCopy(
			Constraint constraint) {
		return new HashMap<>(constraintTotIdToAntsAndAntsCons.get(constraint));
	}

}
