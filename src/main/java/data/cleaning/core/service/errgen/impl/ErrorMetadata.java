package data.cleaning.core.service.errgen.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class ErrorMetadata {
	// Extra info if this record had errors injected. Map contains attr to
	// original attr value.
	private Map<String, String> errorsColsToVal;
	private Map<String, String> origColsToVal;
	private long tid;
	private long gtId;

	public ErrorMetadata() {
		this.errorsColsToVal = new LinkedHashMap<>();
		this.origColsToVal = new LinkedHashMap<>();
	}

	public Map<String, String> getErrorsColsToVal() {
		return errorsColsToVal;
	}

	public void setErrorsColsToVal(Map<String, String> errorsColsToVal) {
		this.errorsColsToVal = errorsColsToVal;
	}

	public void addErrorValForCol(String c, String v) {
		this.errorsColsToVal.put(c, v);
	}

	public void addOrigValForCol(String c, String v) {
		this.origColsToVal.put(c, v);
	}

	public long getTid() {
		return tid;
	}

	public void setTid(long tid) {
		this.tid = tid;
	}

	public long getGtId() {
		return gtId;
	}

	public void setGtId(long gtId) {
		this.gtId = gtId;
	}

	public Map<String, String> getOrigColsToVal() {
		return origColsToVal;
	}

	public void setOrigColsToVal(Map<String, String> origColsToVal) {
		this.origColsToVal = origColsToVal;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((errorsColsToVal == null) ? 0 : errorsColsToVal.hashCode());
		result = prime * result + (int) (gtId ^ (gtId >>> 32));
		result = prime * result
				+ ((origColsToVal == null) ? 0 : origColsToVal.hashCode());
		result = prime * result + (int) (tid ^ (tid >>> 32));
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
		ErrorMetadata other = (ErrorMetadata) obj;
		if (errorsColsToVal == null) {
			if (other.errorsColsToVal != null)
				return false;
		} else if (!errorsColsToVal.equals(other.errorsColsToVal))
			return false;
		if (gtId != other.gtId)
			return false;
		if (origColsToVal == null) {
			if (other.origColsToVal != null)
				return false;
		} else if (!origColsToVal.equals(other.origColsToVal))
			return false;
		if (tid != other.tid)
			return false;
		return true;
	}

	@Override
	public String toString() {
//		return "ErrorMetadata [errorsColsToVal=" + errorsColsToVal
//				+ ", origColsToVal=" + origColsToVal + ", tid=" + tid
//				+ ", gtId=" + gtId + "]";

		return tid + "";
	}

}
