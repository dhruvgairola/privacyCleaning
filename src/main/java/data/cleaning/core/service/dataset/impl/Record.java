package data.cleaning.core.service.dataset.impl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import data.cleaning.core.service.errgen.impl.ErrorMetadata;

public class Record {
	// Record id is a unique identifier to track a record obj and does not exist
	// as an attribute in the dataset. Final keyword for defensive programming.
	private final long id;
	private Map<String, String> colsToVal;
	private ErrorMetadata errMetadata;

	public Record(long id) {
		if (id < 1)
			throw new UnsupportedOperationException(
					"Record id has to be greater than 0.");
		this.id = id;
	}

	public long getId() {
		return id;
	}

	public Map<String, String> getColsToVal() {
		return colsToVal;
	}

	public void setColsToVal(Map<String, String> colsToVal) {
		if (colsToVal instanceof LinkedHashMap) {
			this.colsToVal = colsToVal;

		} else {
			throw new UnsupportedOperationException(
					"Please insert the cols in the order in which they occur in the "
							+ "original dataset.");
		}
	}

	public void modifyValForExistingCol(String col, String val) {
		if (colsToVal.containsKey(col)) {
			colsToVal.put(col, val);
		} else {
			// Don't add anything.
		}
	}

	public ErrorMetadata getErrMetadata() {
		return errMetadata;
	}

	public void setErrMetadata(ErrorMetadata errMetadata) {
		this.errMetadata = errMetadata;
	}

	@Override
	public String toString() {
		return id + "";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((colsToVal == null) ? 0 : colsToVal.hashCode());
		result = prime * result + (int) (id ^ (id >>> 32));
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
		Record other = (Record) obj;
		if (colsToVal == null) {
			if (other.colsToVal != null)
				return false;
		}
		// Check the id also because the dataset could have duplicates.
		if (id != other.id)
			return false;

		for (Entry<String, String> entry : colsToVal.entrySet()) {
			String key = entry.getKey();
			if (!entry.getValue().equals(other.colsToVal.get(key))) {
				return false;
			}
		}

		return true;
	}

	public String prettyPrintRecord(List<String> cols) {
		StringBuilder sb = new StringBuilder();
		sb.append("[" + getId() + "] ");
		Map<String, String> cv = this.colsToVal;
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

	public String getRecordStr(List<String> cols) {
		// TODO: Opt this.
		StringBuilder sb = new StringBuilder();
		Map<String, String> cv = this.colsToVal;
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

	public String getRecordStr(List<String> cols, String separator) {
		// TODO: Opt this.
		StringBuilder sb = new StringBuilder();
		Map<String, String> cv = this.colsToVal;
		if (cols == null) {
			for (Map.Entry<String, String> entry : cv.entrySet()) {
				sb.append(entry.getValue() + separator);
			}
		} else {
			// Column order is given by the constraint.
			for (String col : cols) {
				sb.append(cv.get(col) + separator);
			}
		}

		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();

	}
}
