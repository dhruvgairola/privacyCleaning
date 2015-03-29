package data.cleaning.core.service.matching.impl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

public class Pattern {
	// Master's record id corresponding to pattern.
	private long mRid;
	// Only the attrs involved in the pattern are included, not all the attrs of
	// mRid.
	private Map<String, String> colsToVal;
	private long countInMaster;

	public Pattern() {
		// We need insertion order information.
		this.colsToVal = new LinkedHashMap<>();
		this.countInMaster = -1;
	}

	public long getCountInMaster() {
		return countInMaster;
	}

	public void setCountInMaster(long countInMaster) {
		this.countInMaster = countInMaster;
	}

	public long getmRid() {
		return mRid;
	}

	public void setmRid(long mRid) {
		this.mRid = mRid;
	}

	public Map<String, String> getColsToVal() {
		return colsToVal;
	}

	public void addColAndVal(String col, String val) {
		if (StringUtils.isEmpty(val))
			return;
		this.colsToVal.put(col, val);
	}

	@Override
	public String toString() {
		return prettyPrintPattern(null);
	}

	public String prettyPrintPattern(List<String> cols) {
		StringBuilder sb = new StringBuilder();
		sb.append("[" + getmRid() + "] ");
		Map<String, String> cv = getColsToVal();
		if (cols == null) {
			for (Map.Entry<String, String> entry : cv.entrySet()) {
				sb.append(entry.getValue() + " ");
			}
		} else {
			// Column order is given by the constraint.
			for (String col : cols) {
				sb.append(cv.get(col) + " ");
			}
		}

		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((colsToVal == null) ? 0 : colsToVal.hashCode());
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
		Pattern other = (Pattern) obj;
		if (colsToVal == null) {
			if (other.getColsToVal() != null)
				return false;
		}

		for (Entry<String, String> entry : colsToVal.entrySet()) {
			String key = entry.getKey();
			if (!entry.getValue().equals(other.getColsToVal().get(key))) {
				return false;
			}
		}

		return true;
	}

	public double getSelfInfo(long mSize) {
		return Math
				.log10((double) ((double) mSize / (double) this.countInMaster))
				/ Math.log10(2.0d);
	}
}
