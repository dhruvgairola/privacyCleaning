package data.cleaning.core.utils.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import data.cleaning.core.service.dataset.impl.Constraint;
import data.cleaning.core.service.dataset.impl.InfoContentTable;
import data.cleaning.core.service.dataset.impl.MasterDataset;
import data.cleaning.core.service.dataset.impl.Record;
import data.cleaning.core.service.dataset.impl.TargetDataset;
import data.cleaning.core.service.matching.impl.Match;
import data.cleaning.core.service.repair.impl.Candidate;
import data.cleaning.core.service.repair.impl.NeighbourType;
import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.MaxSizeHashMap;
import data.cleaning.core.utils.Pair;
import data.cleaning.core.utils.ProdLevel;
import data.cleaning.core.utils.Stats;
import data.cleaning.core.utils.Version;
import data.cleaning.core.utils.objectives.IndNormStrategy;

public abstract class Search {

	protected Random rand;
	protected Map<List<Recommendation>, Integer> countCache;
	protected static boolean shdReverseIndStats;
	protected static final Logger logger = Logger.getLogger(Search.class);

	public Search() {
		this.countCache = new MaxSizeHashMap<>(
				Config.SA_REPEAT_NEIGHB_CACHE_SIZE);

		this.rand = new Random(Config.SEED);
	}

	public abstract Set<Candidate> calcOptimalSolns(Constraint constraint,
			List<Match> tgtMatches, TargetDataset tgtDataset,
			MasterDataset mDataset, InfoContentTable table);

	public char[] genRandSig(int randSigSize) {
		if (randSigSize < 1)
			return null;

		char[] randSig = new char[randSigSize];
		boolean ones = false;

		// You should have at least one '1' symbol in a random solution.
		while (!ones) {
			for (int i = 0; i < randSig.length; i++) {
				boolean isOne = rand.nextInt(2) == 1;
				randSig[i] = isOne ? '1' : '0';
				ones = ones || isOne;
			}
		}

		return randSig;

	}

	public PositionalInfo calcPositionalInfo(List<Match> tgtMatches,
			MasterDataset mDataset, Constraint constraint) {

		// Datastructure is built this way for efficiency.
		Map<Integer, Map<Integer, Choice>> positionToChoices = new HashMap<>();
		Map<Integer, Boolean> positionToExactMatches = new HashMap<>();
		Map<Long, Integer> tidToPosition = new HashMap<>();
		List<String> matchedCols = constraint.getColsInConstraint();

		// logger.log(ProdLevel.PROD, "\nRecommendation domain : ");

		int position = 0;
		for (Match tgtMatch : tgtMatches) {

			long tid = tgtMatch.getOriginalrId();
			Queue<Pair<Long, Float>> matches = tgtMatch.getMatchRidToDist();
			Iterator<Pair<Long, Float>> it = matches.iterator();
			Map<Long, Set<String>> matchRidToNonExactCols = tgtMatch
					.getMatchRidToNonExactCols();
			Map<Integer, Choice> choices = new HashMap<>();
			boolean exactMatch = false;
			int choice = 0;

			while (it.hasNext()) {
				Pair<Long, Float> mIdToDist = it.next();
				long mId = mIdToDist.getO1();
				float dist = mIdToDist.getO2();
				List<Recommendation> recs = new ArrayList<>();

				Record master = mDataset.getRecord(mId);
				Map<String, String> colsToVal = master.getColsToVal();

				if (Config.VERSION.contains(Version.REMOVE_EXACT_MATCHES)) {

					Set<String> nonExactCols = matchRidToNonExactCols.get(mId);

					int numNulls = 0;

					for (String m : matchedCols) {
						if (nonExactCols != null && nonExactCols.contains(m)) {
							Recommendation rec = new Recommendation();
							rec.settRid(tid);
							rec.setmRid(mId);

							String val = colsToVal.get(m);
							rec.setCol(m);
							rec.setVal(val);
							recs.add(rec);
						} else {
							// Comment this out if you want exact matches to be
							// considered in a candidate solution.
							recs.add(null);
							numNulls++;
						}
					}

					if (numNulls < recs.size()) {
						// logger.log(ProdLevel.PROD, recs);
						Choice choiceO = new Choice();
						choiceO.setRecs(recs);
						choiceO.setDist(dist);
						choices.put(choice, choiceO);

					} else {
						exactMatch = true;
					}
				} else {

					for (String col : matchedCols) {

						// All cols are exact matches. Basic removal of exact
						// matches.
						if (dist < Config.FLOAT_EQUALIY_EPSILON) {
							recs.add(null);
							exactMatch = true;
						} else {
							Recommendation rec = new Recommendation();
							rec.settRid(tid);
							rec.setmRid(mId);

							String val = colsToVal.get(col);
							rec.setCol(col);
							rec.setVal(val);

							recs.add(rec);
						}

					}

					// logger.log(ProdLevel.PROD, recs);
					Choice choiceO = new Choice();
					choiceO.setRecs(recs);
					choiceO.setDist(dist);
					choices.put(choice, choiceO);

				}

				choice++;
			}

			positionToChoices.put(position, choices);
			positionToExactMatches.put(position, exactMatch);
			tidToPosition.put(tid, position);
			position++;
		}

		PositionalInfo pInfo = new PositionalInfo();
		pInfo.setPositionToChoices(positionToChoices);
		pInfo.setTidToPosition(tidToPosition);
		pInfo.setPositionToExactMatch(positionToExactMatches);

		return pInfo;
	}

	public Candidate getGreedyAnyMatch(int sigSize,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			Map<Long, Integer> tIdToPosition) {
		Candidate can = new Candidate();
		Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
		Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

		int step = sigSize / positionToChoices.keySet().size();
		char[] newSign = new char[sigSize];

		for (int i = 0; i < sigSize; i = i + step) {
			int position = i / step;
			Map<Integer, Choice> choices = positionToChoices.get(position);
			List<Recommendation> recList = new ArrayList<>();

			int choiceNum = 0;

			if (choices != null && !choices.isEmpty())
				choiceNum = rand.nextInt(choices.size());

			Choice choiceObj = choices.get(choiceNum);
			List<Recommendation> choice = null;

			if (choiceObj != null)
				choice = choiceObj.getRecs();

			for (int j = 0; j < step; j++) {
				newSign[i + j] = '1';
				// Possible because (i) no matches or (ii) exact match.
				if (choice != null)
					recList.add(choice.get(j));
			}

			positionToRecs.put(position, recList);
			positionToChoiceNum.put(position, choiceNum);
		}

		can.setPositionToChoiceNum(positionToChoiceNum);
		can.setPositionToRecs(positionToRecs);
		can.setSignature(newSign);
		can.settIdToPosition(tIdToPosition);
		return can;
	}

	public Candidate getGreedyBestMatch(int sigSize,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			Map<Long, Integer> tIdToPosition) {

		Candidate can = new Candidate();
		Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
		Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

		int step = sigSize / positionToChoices.keySet().size();
		char[] newSign = new char[sigSize];

		for (int i = 0; i < sigSize; i = i + step) {
			int position = i / step;
			Map<Integer, Choice> choices = positionToChoices.get(position);
			List<Recommendation> recList = new ArrayList<>();

			int choiceNum = 0;

			// Select the first non exact match.
			for (int bestChoice = 0; bestChoice < choices.keySet().size(); bestChoice++) {
				Choice c = choices.get(bestChoice);
				if(c == null) break;
				
				if (Math.abs(1f - c.getDist()) > Config.FLOAT_EQUALIY_EPSILON) {
					choiceNum = bestChoice;
					break;
				}
			}

			Choice choiceObj = choices.get(choiceNum);
			List<Recommendation> choice = null;

			if (choiceObj != null)
				choice = choiceObj.getRecs();

			for (int j = 0; j < step; j++) {
				newSign[i + j] = '1';
				// Possible because (i) no matches or (ii) exact match.
				if (choice != null)
					recList.add(choice.get(j));
			}

			positionToRecs.put(position, recList);
			positionToChoiceNum.put(position, choiceNum);
		}

		can.setPositionToChoiceNum(positionToChoiceNum);
		can.setPositionToRecs(positionToRecs);
		can.setSignature(newSign);
		can.settIdToPosition(tIdToPosition);
		return can;
	}

	public Candidate getGreedyMostMatch(int sigSize,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			Map<Long, Integer> tIdToPosition) {
		Map<Long, Integer> mIdToCount = new HashMap<>();
		long bestMid = 0;
		int bestCount = Integer.MIN_VALUE;

		for (Map<Integer, Choice> choices : positionToChoices.values()) {
			for (Choice choice : choices.values()) {
				List<Recommendation> recs = null;
				if (choice != null)
					recs = choice.getRecs();

				for (Recommendation r : recs) {
					if (r != null) {
						long key = r.getmRid();
						if (!mIdToCount.containsKey(key)) {
							mIdToCount.put(key, 1);
						} else {
							mIdToCount.put(key, mIdToCount.get(key) + 1);
						}

						if (mIdToCount.get(key) > bestCount) {
							bestCount = mIdToCount.get(key);
							bestMid = key;
						}
					}
				}

			}
		}

		Candidate can = new Candidate();
		Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
		Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

		int step = sigSize / positionToChoices.keySet().size();
		char[] newSign = new char[sigSize];

		for (int i = 0; i < sigSize; i = i + step) {
			int position = i / step;
			Map<Integer, Choice> choices = positionToChoices.get(position);
			List<Recommendation> recList = new ArrayList<>();

			int choiceNum = -1;

			for (Map.Entry<Integer, Choice> choice : choices.entrySet()) {
				int num = choice.getKey();
				long choiceMRid = -1;

				for (Recommendation r : choice.getValue().getRecs()) {
					// Non exact match attr value
					if (r != null) {
						choiceMRid = r.getmRid();
						break;
					}
				}

				if (choiceMRid == bestMid) {
					choiceNum = num;
					break;
				}
			}

			List<Recommendation> choice = null;

			// The greedy id is present as a choice for this position.
			if (choiceNum > -1) {
				Choice choiceObj = choices.get(choiceNum);
				if (choiceObj != null)
					choice = choiceObj.getRecs();
			}

			for (int j = 0; j < step; j++) {
				if (choiceNum == -1) {
					newSign[i + j] = '0';
				} else {
					newSign[i + j] = '1';
					recList.add(choice.get(j));
				}
			}

			if (choiceNum == -1) {
				choiceNum = 0;
			}

			positionToRecs.put(position, recList);
			positionToChoiceNum.put(position, choiceNum);
		}

		can.setPositionToChoiceNum(positionToChoiceNum);
		can.setPositionToRecs(positionToRecs);
		can.setSignature(newSign);
		can.settIdToPosition(tIdToPosition);
		return can;
	}

	/**
	 * Efficient version of getNeighbs for simul annealing.
	 * 
	 * @param currentSoln
	 * @param positionToChoices
	 * @return
	 */
	public Candidate getRandNeighb(int numBitFlipNeighb, int numChoiceNeighb,
			Candidate currentSoln,
			Map<Integer, Map<Integer, Choice>> positionToChoices) {
		Candidate randNeighb = null;

		double bitOrChoice = rand.nextDouble();
		boolean isBit = bitOrChoice <= 0.5;
		int numRandNeighb;
		if (isBit || numChoiceNeighb < 1) {
			numRandNeighb = rand.nextInt(numBitFlipNeighb);
		} else {
			numRandNeighb = numBitFlipNeighb + rand.nextInt(numChoiceNeighb);
		}

		// int numRandNeighb = rand.nextInt(numBitFlipNeighb + numChoiceNeighb);

		// logger.log(DebugLevel.DEBUG, "\n\nTot neighb : "
		// + (numBitFlipNeighb + numChoiceNeighb));
		//
		// logger.log(DebugLevel.DEBUG, "Neighb num : " + numRandNeighb);

		// Bit flip neighb
		if (numRandNeighb + 1 <= numBitFlipNeighb) {
			randNeighb = getRandBitFlipNeighb(currentSoln, positionToChoices,
					numRandNeighb);

			if (randNeighb == null) {
				randNeighb = getRandChoiceNeighb(currentSoln,
						positionToChoices, 0);
			}

		} else {
			// Choice neighb
			randNeighb = getRandChoiceNeighb(currentSoln, positionToChoices,
					numRandNeighb + 1 - numBitFlipNeighb);
		}

		if (randNeighb != null) {
			randNeighb.settIdToPosition(currentSoln.gettIdToPosition());
			randNeighb.setShdReverseInd(currentSoln.isShdReverseInd());
		}

		return randNeighb;
	}

	private Candidate getRandChoiceNeighb(Candidate currentSoln,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			int numRandNeighb) {
		Map<Integer, Integer> currPositionToChoiceNum = currentSoln
				.getPositionToChoiceNumCopy();
		char[] sign = currentSoln.getSignatureCopy();
		int step = sign.length / positionToChoices.keySet().size();

		for (Map.Entry<Integer, Integer> entry : currPositionToChoiceNum
				.entrySet()) {
			int position = entry.getKey();
			int choiceNum = entry.getValue();
			Map<Integer, Choice> choices = positionToChoices.get(position);

			if (numRandNeighb < choices.size()) {
				int num = 0;

				Map<Integer, List<Recommendation>> neighbChoiceNumToRecs = new LinkedHashMap<>();

				for (Map.Entry<Integer, Choice> e : choices.entrySet()) {
					if (numRandNeighb <= num) {
						if (e.getKey() != choiceNum) {
							Choice choiceObj = e.getValue();
							List<Recommendation> neighbChoice = null;
							if (choiceObj != null)
								neighbChoice = choiceObj.getRecs();
							List<Recommendation> filteredNeighbChoice = new ArrayList<>();

							for (int j = 0; j < step; j++) {
								if (sign[position * step + j] == '1') {
									if (j < neighbChoice.size()) {
										Recommendation c = neighbChoice.get(j);
										filteredNeighbChoice.add(c);
									}

								}
							}

							neighbChoiceNumToRecs.put(e.getKey(),
									filteredNeighbChoice);
							break;
						}
					}
					num++;
				}

				for (Map.Entry<Integer, List<Recommendation>> neighbChoiceNumToRec : neighbChoiceNumToRecs
						.entrySet()) {
					int neighbChoiceNum = neighbChoiceNumToRec.getKey();
					List<Recommendation> neighbRecs = neighbChoiceNumToRec
							.getValue();

					Candidate can = new Candidate();

					Map<Integer, List<Recommendation>> neighbPositionToRecs = currentSoln
							.getPositionToRecsCopy();
					Map<Integer, Integer> neighbPositionToChoiceNum = currentSoln
							.getPositionToChoiceNumCopy();

					// Remove the old choice and add the new choice.
					can.setRemoved(neighbPositionToRecs.get(position));
					neighbPositionToRecs.put(position, neighbRecs);
					can.setAdded(neighbRecs);
					neighbPositionToChoiceNum.put(position, neighbChoiceNum);
					can.setPositionToChoiceNum(neighbPositionToChoiceNum);
					can.setPositionToRecs(neighbPositionToRecs);
					can.setSignature(sign);
					can.setNeighbType(NeighbourType.CHOICES);
					can.setNeighbour(true);

					List<Recommendation> neighRecs = can.getRecommendations();
					if (!(neighRecs == null || neighRecs.isEmpty() || neighRecs
							.equals(currentSoln.getRecommendations()))) {
						return can;
					}
				}

				// Do this st this loop is entered again.
				numRandNeighb = 0;
			} else {
				numRandNeighb = numRandNeighb - choices.size();
			}

		}

		return null;
	}

	private Candidate getRandBitFlipNeighb(Candidate currentSoln,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			int numRandNeighb) {
		char[] sign = currentSoln.getSignatureCopy();
		char[][] neighbs = getBitFlipNeighbSigns(sign);

		int step = sign.length / positionToChoices.keySet().size();

		Map<Integer, Integer> currPositionToChoiceNum = currentSoln
				.getPositionToChoiceNumCopy();

		for (int neighbNum = 0; neighbNum < neighbs.length; neighbNum++) {
			if (neighbNum >= numRandNeighb) {
				char[] neighb = neighbs[neighbNum];
				Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
				Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

				Candidate can = new Candidate();

				for (int i = 0; i < neighb.length; i = i + step) {
					int position = i / step;
					List<Recommendation> recsList = new ArrayList<>();
					Map<Integer, Choice> choices = positionToChoices
							.get(position);

					if (currPositionToChoiceNum == null
							|| currPositionToChoiceNum.isEmpty()
							|| !currPositionToChoiceNum.containsKey(position)) {
						continue;
					}

					int choiceNum = currPositionToChoiceNum.get(position);

					Choice choiceObj = choices.get(choiceNum);
					List<Recommendation> choice = null;

					if (choiceObj != null) {
						choice = choiceObj.getRecs();
					}

					for (int j = 0; j < step; j++) {
						if (choice == null) {
							continue;
						}

						if (neighb[i + j] == '1') {
							if (j < choice.size()) {
								Recommendation c = choice.get(j);
								recsList.add(c);
							}

						}

						if (sign[i + j] == '0' && neighb[i + j] == '1') {
							List<Recommendation> diffRec = new ArrayList<>();
							diffRec.add(choice.get(j));
							can.setAdded(diffRec);
						} else if (sign[i + j] == '1' && neighb[i + j] == '0') {
							List<Recommendation> diffRec = new ArrayList<>();
							diffRec.add(choice.get(j));
							can.setRemoved(diffRec);
						}
					}

					positionToRecs.put(position, recsList);
					positionToChoiceNum.put(position, choiceNum);
				}

				can.setPositionToChoiceNum(positionToChoiceNum);
				can.setPositionToRecs(positionToRecs);
				can.setSignature(neighb);
				can.setNeighbType(NeighbourType.BIT_FLIP);
				can.setNeighbour(true);

				List<Recommendation> neighRecs = can.getRecommendations();
				if (!(neighRecs == null || neighRecs.isEmpty() || neighRecs
						.equals(currentSoln.getRecommendations()))) {
					return can;
				}
			}

		}

		return null;
	}

	public Candidate getRand(int sigSize,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			Map<Long, Integer> tidToPosition) {
		char[] randSig = genRandSig(sigSize);

		Candidate can = new Candidate();
		Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
		Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

		int step = randSig.length / positionToChoices.keySet().size();

		for (int i = 0; i < randSig.length; i = i + step) {
			int position = i / step;
			Map<Integer, Choice> choices = positionToChoices.get(position);
			List<Recommendation> recList = new ArrayList<>();

			List<Integer> choicesKey = new ArrayList<>(choices.keySet());
			if (choicesKey == null || choicesKey.isEmpty()) {
				continue;
			}

			int randChoiceNum = rand.nextInt(choicesKey.size());
			Choice choiceObj = choices.get(choicesKey.get(randChoiceNum));
			List<Recommendation> choice = null;
			if (choiceObj != null) {
				choice = choiceObj.getRecs();
			}

			for (int j = 0; j < step; j++) {

				if (randSig[i + j] == '1') {

					if (choice != null && j < choice.size()) {
						recList.add(choice.get(j));
					}

				}
			}

			positionToRecs.put(position, recList);
			positionToChoiceNum.put(position, randChoiceNum);
		}

		can.setPositionToChoiceNum(positionToChoiceNum);
		can.setPositionToRecs(positionToRecs);
		can.settIdToPosition(tidToPosition);
		can.setSignature(randSig);
		return can;
	}

	public Candidate getRandGreedySig(int sigSize,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			Map<Integer, Boolean> positionToExactMatches,
			Map<Long, Integer> tidToPosition) {
		char[] greedySig = new char[sigSize];

		Candidate can = new Candidate();
		Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
		Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

		int step = greedySig.length / positionToChoices.keySet().size();

		for (int i = 0; i < greedySig.length; i = i + step) {
			int position = i / step;
			Map<Integer, Choice> choices = positionToChoices.get(position);
			List<Recommendation> recList = new ArrayList<>();

			List<Integer> choicesKey = new ArrayList<>(choices.keySet());
			if (choicesKey == null || choicesKey.isEmpty()) {
				continue;
			}

			List<Recommendation> choice = null;

			int randChoiceNum = rand.nextInt(choicesKey.size());
			Choice choiceObj = choices.get(choicesKey.get(randChoiceNum));
			if (choiceObj != null)
				choice = choiceObj.getRecs();
			double shdAddMatch = rand.nextDouble();
			double dist = choiceObj.getDist();

			if (shdAddMatch < dist) {
				for (int j = 0; j < step; j++) {

					if (choice != null && j < choice.size()) {
						greedySig[i + j] = '1';
						recList.add(choice.get(j));
					}
				}
			}

			positionToRecs.put(position, recList);
			positionToChoiceNum.put(position, randChoiceNum);
		}

		can.setPositionToChoiceNum(positionToChoiceNum);
		can.setPositionToRecs(positionToRecs);
		can.settIdToPosition(tidToPosition);
		can.setSignature(greedySig);
		return can;
	}

	public Set<Candidate> getBitFlipNeighbs(Candidate currentSoln,
			Map<Integer, Map<Integer, Choice>> positionToChoices) {
		char[] sign = currentSoln.getSignatureCopy();
		char[][] neighbs = getBitFlipNeighbSigns(sign);

		Set<Candidate> bitFlipNeighbs = new HashSet<>();
		int step = sign.length / positionToChoices.keySet().size();

		Map<Integer, Integer> currPositionToChoiceNum = currentSoln
				.getPositionToChoiceNumCopy();

		for (char[] neighb : neighbs) {

			Map<Integer, List<Recommendation>> positionToRecs = new LinkedHashMap<>();
			Map<Integer, Integer> positionToChoiceNum = new LinkedHashMap<>();

			Candidate can = new Candidate();

			for (int i = 0; i < neighb.length; i = i + step) {
				int position = i / step;
				List<Recommendation> recsList = new ArrayList<>();
				Map<Integer, Choice> choices = positionToChoices.get(position);

				if (currPositionToChoiceNum == null
						|| currPositionToChoiceNum.isEmpty()
						|| !currPositionToChoiceNum.containsKey(position)) {
					continue;
				}

				int choiceNum = currPositionToChoiceNum.get(position);

				Choice choiceObj = choices.get(choiceNum);
				List<Recommendation> choice = null;

				if (choiceObj != null)
					choice = choices.get(choiceNum).getRecs();

				for (int j = 0; j < step; j++) {
					if (choice == null) {
						continue;
					}

					if (neighb[i + j] == '1') {
						if (j < choice.size()) {
							Recommendation c = choice.get(j);
							recsList.add(c);
						}

					}

					if ((sign[i + j] == '0' && neighb[i + j] == '1')
							|| (sign[i + j] == '1' && neighb[i + j] == '0')) {
						List<Recommendation> diffRec = new ArrayList<>();
						diffRec.add(choice.get(j));
						can.setAdded(diffRec);
					}
				}

				positionToRecs.put(position, recsList);
				positionToChoiceNum.put(position, choiceNum);
			}

			can.setPositionToChoiceNum(positionToChoiceNum);
			can.setPositionToRecs(positionToRecs);
			can.setSignature(neighb);
			can.setNeighbType(NeighbourType.BIT_FLIP);
			can.setNeighbour(true);

			List<Recommendation> neighRecs = can.getRecommendations();
			if (!(neighRecs == null || neighRecs.isEmpty() || neighRecs
					.equals(currentSoln.getRecommendations()))) {
				bitFlipNeighbs.add(can);
			}

		}

		return bitFlipNeighbs;
	}

	/**
	 * A neighbor is only 1 bit different from the current signature.
	 * 
	 * @param currentSig
	 * @return
	 */
	public char[][] getBitFlipNeighbSigns(char[] currentSig) {
		char[][] neighbours = new char[currentSig.length][currentSig.length];

		for (int i = 0; i < currentSig.length; i++) {
			char[] n = Arrays.copyOf(currentSig, currentSig.length);
			n[i] = currentSig[i] == '1' ? '0' : '1';
			neighbours[i] = n;
		}

		return neighbours;
	}

	/**
	 * A neighbourhood of some current soln is the amalgamation of the
	 * "bit flip" neighbourhood with the "choice" neighbourhood. A bit flip
	 * neighbourhood of a solution "1011" consists of "0011", "1111", "1001",
	 * "1010" i.e., everything that differs by 1 bit. Given a recommendation
	 * space e.g., [(t1, m1, FName, Dhruv), (t1, m1, LName, Gairola),(t2, m1,
	 * FName, Dhruv), (t2, m1, LName, Gairola)], the solution "1011" refers to
	 * [(t1, m1, FName, Dhruv), (t2, m1, FName, Dhruv), (t2, m1, LName,
	 * Gairola)] i.e., every element in the recommendation list corresponds to a
	 * bit.
	 * 
	 * A choice neighbourhood exists in the cases where a target tuple matches
	 * multiple master tuples. For a recommendation space- [(t1, m1, FName,
	 * Dhruv), (t1, m1, LName, Gairola),(t1, m2, FName, Drew), (t2, m2, LName,
	 * Gairola),(t2, m1, FName, Dhruv), (t2, m1, LName, Gairola)], either m1 or
	 * m2's vals should be recommended for t1, not both. In this case, "1011"
	 * could correspond to either [(t1, m1, FName, Dhruv), (t2, m1, FName,
	 * Dhruv), (t2, m1, LName, Gairola)] or [(t1, m2, FName, Drew), (t2, m1,
	 * FName, Dhruv), (t2, m1, LName, Gairola)]. Hence, these two fall within
	 * each others neighbourhoods.
	 * 
	 * We need to account for both the bit flip and choice neighbourhoods
	 * together so that our search algorithms can explore a large solution
	 * space. If we only work with one type of neighbourhood, then a significant
	 * part of the solution space will not be explored, and it is almost certain
	 * that you will not get good solutions i.e., a pure bit flip solution will
	 * never explore the alternative matches, while a pure "choice"
	 * neighbourhood solution will never explore different permutations of the
	 * current solution.
	 * 
	 * @param currentSoln
	 * @param positionToChoices
	 * @return
	 */
	public Set<Candidate> getNeighbs(Candidate currentSoln,
			Map<Integer, Map<Integer, Choice>> positionToChoices) {
		Set<Candidate> neighbSolns = new HashSet<>();

		Set<Candidate> bitFlipN = getBitFlipNeighbs(currentSoln,
				positionToChoices);

		Set<Candidate> choiceN = getChoiceNeighbs(currentSoln,
				positionToChoices);

		// logger.log(SecurityLevel.PROD, "\nNeighbourhood (Bit flip) : "
		// + bitFlipN);
		// logger.log(SecurityLevel.PROD, "\nNeighbourhood (Choice) : "
		// + choiceN);

		neighbSolns.addAll(bitFlipN);
		neighbSolns.addAll(choiceN);

		for (Candidate n : neighbSolns) {
			if (n != null)
				n.settIdToPosition(currentSoln.gettIdToPosition());
		}

		return neighbSolns;
	}

	// In the paper, these are known as match neighbours.
	private Set<Candidate> getChoiceNeighbs(Candidate currentSoln,
			Map<Integer, Map<Integer, Choice>> positionToChoices) {

		Set<Candidate> choiceNeighbs = new HashSet<>();
		Map<Integer, Integer> currPositionToChoiceNum = currentSoln
				.getPositionToChoiceNumCopy();
		char[] sign = currentSoln.getSignatureCopy();
		int step = sign.length / positionToChoices.keySet().size();

		for (Map.Entry<Integer, Integer> entry : currPositionToChoiceNum
				.entrySet()) {
			int position = entry.getKey();
			int choiceNum = entry.getValue();
			Map<Integer, Choice> choices = positionToChoices.get(position);

			Map<Integer, List<Recommendation>> neighbChoiceNumToRecs = new LinkedHashMap<>();

			for (Map.Entry<Integer, Choice> e : choices.entrySet()) {
				if (e.getKey() != choiceNum) {
					Choice choiceObj = e.getValue();
					List<Recommendation> neighbChoice = null;
					if (choiceObj != null) {
						neighbChoice = choiceObj.getRecs();
					}

					List<Recommendation> filteredNeighbChoice = new ArrayList<>();

					for (int j = 0; j < step; j++) {
						if (sign[position * step + j] == '1') {
							if (j < neighbChoice.size()) {
								Recommendation c = neighbChoice.get(j);
								filteredNeighbChoice.add(c);
							}

						}
					}

					neighbChoiceNumToRecs.put(e.getKey(), filteredNeighbChoice);
				}
			}

			for (Map.Entry<Integer, List<Recommendation>> neighbChoiceNumToRec : neighbChoiceNumToRecs
					.entrySet()) {
				int neighbChoiceNum = neighbChoiceNumToRec.getKey();
				List<Recommendation> neighbRecs = neighbChoiceNumToRec
						.getValue();

				Candidate can = new Candidate();

				Map<Integer, List<Recommendation>> neighbPositionToRecs = currentSoln
						.getPositionToRecsCopy();
				Map<Integer, Integer> neighbPositionToChoiceNum = currentSoln
						.getPositionToChoiceNumCopy();

				// Remove the old choice and add the new choice.
				can.setRemoved(neighbPositionToRecs.get(position));
				neighbPositionToRecs.put(position, neighbRecs);
				can.setAdded(neighbRecs);
				neighbPositionToChoiceNum.put(position, neighbChoiceNum);
				can.setPositionToChoiceNum(neighbPositionToChoiceNum);
				can.setPositionToRecs(neighbPositionToRecs);
				can.setSignature(sign);
				can.setNeighbType(NeighbourType.CHOICES);
				can.setNeighbour(true);

				List<Recommendation> neighRecs = can.getRecommendations();

				if (!(neighRecs == null || neighRecs.isEmpty() || neighRecs
						.equals(currentSoln.getRecommendations()))) {
					choiceNeighbs.add(can);
				}
			}

		}

		return choiceNeighbs;
	}

	public double calcMaxInd(Constraint constraint, List<Record> records,
			IndNormStrategy indNormStrat) {
		if (records == null || records.isEmpty() || constraint == null)
			return 0d;

		if (indNormStrat == IndNormStrategy.THEORETICAL_MAX) {
			return Math.log(records.size()) / Math.log(2d);
		} else if (indNormStrat == IndNormStrategy.PATTERN_BASED) {

			List<String> cols = constraint.getColsInConstraint();

			if (cols == null || cols.isEmpty())
				return 0;

			Set<String> patterns = new HashSet<>();

			for (Record record : records) {
				String key = record.getRecordStr(cols);
				patterns.add(key);
			}

			return Math.log(patterns.size()) / Math.log(2d);
		} else {
			return Stats.ind(constraint, records);
		}

	}

	protected Candidate getInitialSoln(InitStrategy strategy, int sigSize,
			Map<Integer, Map<Integer, Choice>> positionToChoices,
			Map<Integer, Boolean> positionToExactMatches,
			Map<Long, Integer> tIdToPosition) {
		Candidate initialSoln = null;

		if (strategy == InitStrategy.GREEDY_MOST_MATCH) {
			initialSoln = getGreedyMostMatch(sigSize, positionToChoices,
					tIdToPosition);
		} else if (strategy == InitStrategy.GREEDY_ANY_MATCH) {
			initialSoln = getGreedyAnyMatch(sigSize, positionToChoices,
					tIdToPosition);
		} else if (strategy == InitStrategy.GREEDY_BEST_MATCH) {
			initialSoln = getGreedyBestMatch(sigSize, positionToChoices,
					tIdToPosition);
		} else if (strategy == InitStrategy.RANDOM) {
			initialSoln = getRand(sigSize, positionToChoices, tIdToPosition);
		} else if (strategy == InitStrategy.RANDOM_GREEDY_SIG) {
			initialSoln = getRandGreedySig(sigSize, positionToChoices,
					positionToExactMatches, tIdToPosition);
		} else {
			initialSoln = getGreedyBestMatch(sigSize, positionToChoices,
					tIdToPosition);
		}

		logger.log(ProdLevel.PROD, "\n\n" + strategy + ", Initial soln : "
				+ initialSoln.getRecommendations());

		return initialSoln;
	}

}
