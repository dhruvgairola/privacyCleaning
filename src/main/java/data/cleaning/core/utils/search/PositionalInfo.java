package data.cleaning.core.utils.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.cleaning.core.service.repair.impl.Recommendation;

/**
 * A position refers to specific chunk of the solution signature. e.g., if the
 * solution signature is 100010111, and assuming |attr(FD)| = 3, then position 1
 * refers to "100", position 2 refers to "010" while position 3 refers to "111".
 * 
 * @author dhruvgairola
 *
 */
public class PositionalInfo {
	// Each position in a signature corresponds a bunch of matches i.e.,
	// "choices".
	private Map<Integer, Map<Integer, Choice>> positionToChoices;
	// positionToChoices does not have exact matches. Hence, we have a secondary
	// structure to signal exact matches.
	private Map<Integer, Boolean> positionToExactMatch;
	// Each position really corresponds to a target id.
	private Map<Long, Integer> tidToPosition;

	public PositionalInfo() {
		this.positionToChoices = new HashMap<>();
		this.positionToExactMatch = new HashMap<>();
		this.tidToPosition = new HashMap<>();
	}

	public Map<Integer, Map<Integer, Choice>> getPositionToChoices() {
		return positionToChoices;
	}

	public void setPositionToChoices(
			Map<Integer, Map<Integer, Choice>> positionToChoices) {
		this.positionToChoices = positionToChoices;
	}

	public Map<Long, Integer> getTidToPosition() {
		return tidToPosition;
	}

	public void setTidToPosition(Map<Long, Integer> tidToPosition) {
		this.tidToPosition = tidToPosition;
	}

	public Map<Integer, Boolean> getPositionToExactMatch() {
		return positionToExactMatch;
	}

	public void setPositionToExactMatch(
			Map<Integer, Boolean> positionToExactMatch) {
		this.positionToExactMatch = positionToExactMatch;
	}

}
