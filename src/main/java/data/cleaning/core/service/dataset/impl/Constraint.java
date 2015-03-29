package data.cleaning.core.service.dataset.impl;

import java.util.ArrayList;
import java.util.List;

public class Constraint {

	private long id;
	private long datasetid;
	private String antecedent;// This can be multiple attributes (in csv)
	private String consequent;
	private boolean isReverse;// Is this a reverse of an existing constraint?

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getDatasetid() {
		return datasetid;
	}

	public void setDatasetid(long datasetid) {
		this.datasetid = datasetid;
	}

	public String getAntecedent() {
		return antecedent;
	}

	public void setAntecedent(String antecedent) {
		this.antecedent = antecedent;
	}

	public String getConsequent() {
		return consequent;
	}

	public void setConsequent(String consequent) {
		this.consequent = consequent;
	}

	public boolean isReverse() {
		return isReverse;
	}

	public void setReverse(boolean isReverse) {
		this.isReverse = isReverse;
	}

	@Override
	public String toString() {
		return antecedent + " -> " + consequent;
	}

	public List<String> getAntecedentCols() {
		List<String> ants = new ArrayList<>();
		String[] antsArr = getAntecedent().split(",");
		for (String a : antsArr) {
			ants.add(a.trim());
		}
		return ants;
	}

	public List<String> getConsequentCols() {
		List<String> cons = new ArrayList<>();
		String[] consArr = getConsequent().split(",");
		for (String c : consArr) {
			cons.add(c.trim());
		}
		return cons;
	}

	public List<String> getColsInConstraint() {
		List<String> cols = new ArrayList<>(getAntecedentCols());
		cols.addAll(getConsequentCols());
		return cols;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((antecedent == null) ? 0 : antecedent.hashCode());
		result = prime * result
				+ ((consequent == null) ? 0 : consequent.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Constraint other = (Constraint) obj;
		if (antecedent == null) {
			if (other.antecedent != null)
				return false;
		} else if (!antecedent.equals(other.antecedent))
			return false;
		if (consequent == null) {
			if (other.consequent != null)
				return false;
		} else if (!consequent.equals(other.consequent))
			return false;
		return true;
	}

}