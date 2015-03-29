package data.cleaning.core.service.repair.impl;

public class Recommendation {
	private long mRid;
	private long tRid;
	private String col;
	private String val;

	public long gettRid() {
		return tRid;
	}

	public void settRid(long tRid) {
		this.tRid = tRid;
	}

	public long getmRid() {
		return mRid;
	}

	public void setmRid(long mRid) {
		this.mRid = mRid;
	}

	public String getCol() {
		return col;
	}

	public void setCol(String col) {
		this.col = col;
	}

	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((col == null) ? 0 : col.hashCode());
		result = prime * result + (int) (mRid ^ (mRid >>> 32));
		result = prime * result + (int) (tRid ^ (tRid >>> 32));
		result = prime * result + ((val == null) ? 0 : val.hashCode());
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
		Recommendation other = (Recommendation) obj;
		if (col == null) {
			if (other.col != null)
				return false;
		} else if (!col.equals(other.col))
			return false;
		if (mRid != other.mRid)
			return false;
		if (tRid != other.tRid)
			return false;
		if (val == null) {
			if (other.val != null)
				return false;
		} else if (!val.equals(other.val))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "(t" + tRid + ",m" + mRid + "," + col + "," + val + ")";
	}

}
