package data.cleaning.core.service.matching.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.math.Arrays;

/**
 * Referred to as P_{str} in the SIGMOD 07 paper (Scannapieco). This structure
 * is completely private (you shouldn't be able to make sense of the information
 * held by this object by printing it).
 * 
 * @author dhruvgairola
 * 
 */
public class EmbPrivateDataset {
	private long datasetId;
	private boolean isMaster;
	private String[] cols;
	// TODO: Looping on cols (outer) and rows (inner) is slower that on rows and
	// cols.
	private EmbVector[][] vectTable;
	private Map<Integer, Long> rowIdToRId;

	public EmbPrivateDataset(int numRecords, int numAttrs) {
		this.vectTable = new EmbVector[numRecords][numAttrs];
		this.rowIdToRId = new HashMap<Integer, Long>();
	}

	public long getDatasetId() {
		return datasetId;
	}

	// public Map<Integer, Long> getRowIdToRId() {
	// return rowIdToRId;
	// }

	public long getRId(int rowId) {
		return rowIdToRId.get(rowId);
	}

	// DONT uncomment : the addCoordinates takes care of this
	// public void setRowIdToRId(Map<Integer, Long> rowIdToRId) {
	// this.rowIdToRId = rowIdToRId;
	// }

	public boolean isMaster() {
		return isMaster;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}

	public void setDatasetId(long datasetId) {
		this.datasetId = datasetId;
	}

	public EmbVector[][] getVectTable() {
		return vectTable;
	}

	public void setVectTable(EmbVector[][] vectTable) {
		this.vectTable = vectTable;
	}

	public void addVector(long rId, int r, int c, EmbVector v) {
		this.vectTable[r][c] = v;
		this.rowIdToRId.put(r, rId);
	}

	public String[] getCols() {
		return cols;
	}

	public void setCols(String[] cols) {
		this.cols = cols;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (int row = 0; row < vectTable.length; row++) {
			EmbVector[] col = vectTable[row];
			sb.append(Arrays.toString(col) + "\n");

		}
		return (isMaster() ? "Master[datasetId=" : "Target[datasetId=")
				+ datasetId + "] : " + sb;
	}

}
