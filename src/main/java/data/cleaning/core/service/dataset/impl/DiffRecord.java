package data.cleaning.core.service.dataset.impl;

import java.util.Map;

public class DiffRecord {
	private long id;
	private int timestamp;
	// Store only the values which are different form the original record.
	private Map<String, String> diffColsToVal;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, String> getDiffColsToVal() {
		return diffColsToVal;
	}

	public void setDiffColsToVal(Map<String, String> diffColsToVal) {
		this.diffColsToVal = diffColsToVal;
	}

}
