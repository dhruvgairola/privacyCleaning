package data.cleaning.core.utils.objectives;

import org.apache.log4j.Logger;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.repair.impl.Candidate;

/**
 * All objective functions maintain state and store past calculations. This is
 * for optimization purposes since past calculations can be used to process
 * future calculations. However, this makes the client code more tedious to
 * construct because we need to keep creating new objective objects for
 * different constraints.
 * 
 * @author dhruvgairola
 *
 */
public abstract class Objective {
	protected boolean shouldNormalize;
	protected double epsilon;
	protected double weight;
	protected double prevBestOut;
	protected Constraint constraint;
	protected InfoContentTable table;
	protected static final Logger logger = Logger.getLogger(Objective.class);

	public Objective(double epsilon, double weight, boolean shouldNormalize,
			Constraint constraint, InfoContentTable table) {
		this.epsilon = epsilon;
		this.weight = weight;
		this.constraint = constraint;
		this.table = table;
		this.shouldNormalize = shouldNormalize;
	}

	public abstract double out(Candidate input, TargetDataset tgtDataset,
			MasterDataset mDataset, double maxPvt, double maxInd, long maxSize);

	public double getEpsilon() {
		return this.epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	public double getWeight() {
		return this.weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public double getPrevBestOut() {
		return this.prevBestOut;
	}

	public void setPrevBestOut(double prevBestOut) {
		this.prevBestOut = prevBestOut;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((this.getClass().getName() == null) ? 0 : this.getClass()
						.getName().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		return this.getClass().getName().equals(obj.getClass().getName());
	}

}
