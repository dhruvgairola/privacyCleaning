package data.cleaning.core.service.repair.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.Record;

// Records which agree in LHS but not RHS
public class Violations {
	// String is the LHS (antecedent)
	private Multimap<String, Record> violMap;
	private Constraint constraint;

	public Violations() {
		this.violMap = ArrayListMultimap.create();
	}

	public Constraint getConstraint() {
		return constraint;
	}

	public void setConstraint(Constraint constraint) {
		this.constraint = constraint;
	}

	@Override
	public String toString() {
		return violMap.toString();
	}

	public Multimap<String, Record> getViolMap() {
		return violMap;
	}

	public void setViolMap(Multimap<String, Record> violMap) {
		this.violMap = violMap;
	}

}
