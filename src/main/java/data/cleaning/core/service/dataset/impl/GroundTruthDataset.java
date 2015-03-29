package data.cleaning.core.service.dataset.impl;

import java.util.List;

public class GroundTruthDataset extends Dataset {
	// Corresponding target dataset id
	private long tid;

	public GroundTruthDataset(List<Record> records) {
		super(records);
		setName("Ground truth");
	}

	public long getTid() {
		return tid;
	}

	public void setTid(long tid) {
		this.tid = tid;
	}

	@Override
	public String toString() {
		return getName() + ": " + getRecords();
	}

}
