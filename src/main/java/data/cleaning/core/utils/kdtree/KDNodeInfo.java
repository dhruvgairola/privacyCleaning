package data.cleaning.core.utils.kdtree;

import java.util.List;

import data.cleaning.core.service.matching.impl.EmbVector;

public class KDNodeInfo {
	private long rId;
	private int rowId;
	private EmbVector embVector;
	// Hack : secondary structure which is a partitioned version of embVector.
	// Used only when Config.SHOULD_AVERAGE is true.
	private List<EmbVector> chunkedEmbVector;

	public long getrId() {
		return rId;
	}

	public void setrId(long rId) {
		this.rId = rId;
	}

	public void setEmbVect(EmbVector embVector) {
		this.embVector = embVector;
	}

	public EmbVector getEmbVector() {
		return this.embVector;
	}

	public int getRowId() {
		return rowId;
	}

	public void setRowId(int rowId) {
		this.rowId = rowId;
	}

	public List<EmbVector> getChunkedEmbVector() {
		return chunkedEmbVector;
	}

	public void setChunkedEmbVector(List<EmbVector> chunkedEmbVector) {
		this.chunkedEmbVector = chunkedEmbVector;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((embVector == null) ? 0 : embVector.hashCode());
		result = (int) (prime * result + rId);
		result = prime * result + rowId;
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
		KDNodeInfo other = (KDNodeInfo) obj;
		if (embVector == null) {
			if (other.embVector != null)
				return false;
		} else if (!embVector.equals(other.embVector))
			return false;
		if (rId != other.rId)
			return false;
		if (rowId != other.rowId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KDNodeInfo [rId=" + rId + ", embVector=" + embVector + "]";
	}

}
