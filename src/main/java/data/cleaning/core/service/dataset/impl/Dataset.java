package data.cleaning.core.service.dataset.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Dataset {

	private long id;
	private String name;
	private String url; // File location
	private List<Constraint> constraints;
	private List<Record> records;

	public Dataset(List<Record> records) {
		this.constraints = new ArrayList<>();

		Collections.sort(records, new Comparator<Record>() {

			@Override
			public int compare(Record o1, Record o2) {
				if (o1.getId() > o2.getId())
					return 1;
				else if (o1.getId() < o2.getId())
					return -1;
				else
					return 0;
			}
		});

		this.records = records;
	}

	public Record getRecord(long rid) {
		// Use index for O(1) lookup. Possible bec the underlying records are
		// sorted by an immutable record id within an unmodifiable list.
		return records == null || records.isEmpty() ? null : records
				.get((int) rid - 1);
	}

	public List<Record> getRecords() {
		return this.records;
	}

	public List<Constraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(List<Constraint> constraints) {
		this.constraints = constraints;
	}

	public void addConstraint(Constraint constraint) {
		this.constraints.add(constraint);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
