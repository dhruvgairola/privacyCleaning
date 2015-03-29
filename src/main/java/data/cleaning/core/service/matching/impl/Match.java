package data.cleaning.core.service.matching.impl;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.Pair;

/**
 * Please delete any file cache in the filecache folder when this class is
 * modified.
 * 
 * @author dhruvgairola
 *
 */
public class Match {
	// Just some unique id.
	private long matchId;
	private long originalrId;
	// private Map<Long, Float> matchRidToDist;
	private NavigableMap<Float, Long> distToMatchRid;
	private Map<Long, Set<String>> matchRidToNonExactCols;
	private Set<Long> bestMatches;
	private Queue<Pair<Long, Float>> matchQ;
	private double bestDist;

	public Match() {
		// this.matchRidToDist = new
		// MaxSizeHashMap<>(Config.MAX_MATCHES_ALLOWED);
		this.matchRidToNonExactCols = new HashMap<>();
		this.bestMatches = new HashSet<>();
		this.bestDist = Double.MAX_VALUE;
		// Red-black tree to handle floats.
		this.distToMatchRid = new TreeMap<>();
		this.matchQ = new PriorityQueue<>(Config.TOP_K_MATCHES,
				new Comparator<Pair<Long, Float>>() {

					@Override
					public int compare(Pair<Long, Float> o1,
							Pair<Long, Float> o2) {
						if (o1.getO2() > o2.getO2()) {
							return -1;
						} else if (o2.getO2() > o1.getO2()) {
							return 1;
						} else {
							return 0;
						}
					}
				});
	}

	public long getMatchId() {
		return matchId;
	}

	public void setMatchId(long matchId) {
		this.matchId = matchId;
	}

	public long getOriginalrId() {
		return originalrId;
	}

	public void setOriginalrId(long originalrId) {
		this.originalrId = originalrId;
	}

	public void addRidAndDistIfAcceptable(long rId, float dist,
			boolean shouldRemoveDupMatches) {
		if (shouldRemoveDupMatches) {
			// RB tree search- O(log n)
			Float c = distToMatchRid.ceilingKey(dist);
			Float f = distToMatchRid.floorKey(dist);

			if (c == null && f == null) {
				insert(rId, dist);
			} else if (c == null && f != null) {
				if (dist - f.floatValue() > Config.FLOAT_EQUALIY_EPSILON) {
					insert(rId, dist);
				}
			} else if (f == null && c != null) {
				if (c.floatValue() - dist > Config.FLOAT_EQUALIY_EPSILON) {
					insert(rId, dist);
				}
			} else {
				if (dist - f.floatValue() > Config.FLOAT_EQUALIY_EPSILON
						|| c.floatValue() - dist > Config.FLOAT_EQUALIY_EPSILON) {
					insert(rId, dist);
				}
			}
		} else {
			insert(rId, dist);
		}
	}

	private void insert(long rId, float dist) {
		if (dist < bestDist) {
			bestDist = dist;
			bestMatches.removeAll(bestMatches);
			bestMatches.add(rId);
		} else if (dist == bestDist) {
			bestMatches.add(rId);
		}

		Pair<Long, Float> p = new Pair<>();
		p.setO1(rId);
		p.setO2(dist);

		Pair<Long, Float> worst = matchQ.peek();

		if (matchQ.size() < Config.TOP_K_MATCHES) {
			matchQ.offer(p);
		} else if (matchQ.size() >= Config.TOP_K_MATCHES
				&& dist < worst.getO2()) {
			matchQ.poll();
			matchQ.offer(p);
		}

		distToMatchRid.put(dist, rId);
	}

	public void addRidAndNonExactCol(long rId, String col) {
		if (matchRidToNonExactCols.containsKey(rId)) {
			Set<String> nec = matchRidToNonExactCols.get(rId);
			nec.add(col);
		} else {
			Set<String> nec = new HashSet<>();
			nec.add(col);
			matchRidToNonExactCols.put(rId, nec);
		}
	}

	public Queue<Pair<Long, Float>> getMatchRidToDist() {
		return matchQ;
	}

	public Map<Long, Set<String>> getMatchRidToNonExactCols() {
		return matchRidToNonExactCols;
	}

	public Set<Long> getBestMatches() {
		return bestMatches;
	}

	public double getDistOfBestMatch() {
		return this.bestDist;
	}

	// public List<String> getCols() {
	// if (matchRidToDistCoords.isEmpty())
	// return null;
	//
	// Map<String, Double> bestCoord = matchRidToDistCoords
	// .get(getBestMatchrId());
	// return new ArrayList<>(bestCoord.keySet());
	// }

	@Override
	public String toString() {
		return "Match [rId=" + originalrId + ", otherRid=" + matchQ + "]";
		// return "Match [rId=" + originalrId + ", otherRid=" + matchRidToDist
		// + ", matchRidToDistCoords=" + matchRidToDistCoords + "]";
	}

}
