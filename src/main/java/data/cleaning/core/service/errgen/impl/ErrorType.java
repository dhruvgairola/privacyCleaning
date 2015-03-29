package data.cleaning.core.service.errgen.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ErrorType {
	private Type type;
	private float percentage;
	private Map<Float, Float> simToDistribution;
	private List<String> specialChars;

	public ErrorType(Type type, float percentage) {
		this.type = type;
		this.percentage = percentage;
		this.specialChars = new ArrayList<>();
	}

	public Type getType() {
		return type;
	}

	public Map<Float, Float> getSimToDistribution() {
		return simToDistribution;
	}

	public void setSimToDistribution(Map<Float, Float> simToDistribution) {
		this.simToDistribution = simToDistribution;
	}

	public float getPercentage() {
		return percentage;
	}

	public List<String> getSpecialChars() {
		return specialChars;
	}

	public void setSpecialChars(List<String> specialChars) {
		this.specialChars = specialChars;
	}

	public enum Type {
		IN_DOMAIN_SIMILAR, IN_DOMAIN, OUTSIDE_DOMAIN_SIMILAR, OUTSIDE_DOMAIN, SPECIAL_CHARS;
	}
}
