package data.cleaning.core.service.matching.impl;

import java.util.Arrays;

public class EmbVector {
	private float[] vector;
	// Used for debugging only.
	private String meta;
	private String value;

	public float[] getVector() {
		return vector;
	}

	public void setVector(float[] vector) {
		this.vector = vector;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(vector);
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
		EmbVector other = (EmbVector) obj;
		if (!Arrays.equals(vector, other.vector))
			return false;
		return true;
	}

	public String getMeta() {
		return meta;
	}

	public void setMeta(String meta) {
		this.meta = meta;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return Arrays.toString(vector);
	}
}
