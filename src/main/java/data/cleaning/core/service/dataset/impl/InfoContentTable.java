package data.cleaning.core.service.dataset.impl;

import java.util.Arrays;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class InfoContentTable {

	private double[][] data;
	private BiMap<Integer, String> colIdToName;

	public InfoContentTable() {
		this.colIdToName = HashBiMap.create();
	}

	public BiMap<Integer, String> getColIdToName() {
		return colIdToName;
	}

	public void setColIdToName(BiMap<Integer, String> colIdToName) {
		this.colIdToName = colIdToName;
	}

	public double[][] getData() {
		return data;
	}

	public void setData(double[][] data) {
		this.data = data;
	}

	/**
	 * Relatively expensive call.
	 * @return
	 */
	public double getMaxInfoContent() {
		double maxInfoContent = 0d;

		for (int row = 0; row < data.length; row++) {
			for (int col = 0; col < data[0].length; col++) {
				maxInfoContent += data[row][col];
			}
		}

		return maxInfoContent;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < data.length; i++) {
			sb.append(Arrays.toString(data[i])+"\n");
		}
		
		return sb.toString();
	}
}
