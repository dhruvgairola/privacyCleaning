------------------------------------------------------------------------
1. Requirements for running project
------------------------------------------------------------------------
Java 7 64-bit, Maven 3.0.3 (m2eclipse plugin if you plan to use Eclipse)

------------------------------------------------------------------------
2. Datasets
------------------------------------------------------------------------
Datasets for our experiments (IMDB and books)
-----------------------
Download link :
https://drive.google.com/file/d/0B6gIlEKnOlv1UHQxcUlLR25ncnc/view?usp=sharing

Unzip the files into the root directory.

Adding a new dataset
-----------------------
Only csv files supported currently. The first line of the csv file has to have the attribute names. Modify 
```
vim src/main/java/data/cleaning/core/utils/Config.java
```
and add the path to your new dataset file (following the examples for the IMDB and books datasets).

Adding a new FD file
-----------------------
If you have an FD of the form "A, B -> C", then convert this "A, B, C" and add to a csv file. Without loss of generality, this requires that your FDs have been decomposed appropriately (only 1 consequent). Modify 
```
vim src/main/java/data/cleaning/core/utils/Config.java
```
and add the path to your new fd file (following the examples for the IMDB and books datasets).

------------------------------------------------------------------------
3. Thesis
------------------------------------------------------------------------
The link for my thesis : http://macsphere.mcmaster.ca/bitstream/11375/18075/2/gairola_dhruv_201507_msc_computer_science.pdf

------------------------------------------------------------------------
4. General comments
------------------------------------------------------------------------
```
mvn clean install
```
Run above everytime before running any experiments (if code was changed since the last time).

The code was implemented using the Spring dependency injection framework. Functionality is implemented as services, while experiments are implemented as JUnit tests which call the services.

In our code, the the constrained approach is referred to as the epsilon approach, the dynamic approach is referred to as the epsilon dynamic approach, the hierarchical approach is referred to as the epsilon lexical approach while the weighted approach is referred to as the weighted approach (no change here). 

------------------------------------------------------------------------
5. Replicating experiments
------------------------------------------------------------------------

To replicate the experiments in our paper, you need to checkout the relevant branch. Every branch has 1 commit in its commit history which is tagged with the following naming convention- "expt_\*", where "\*" represents some unique identifier. You need to have the HEAD of your checkedout branch pointing at the tagged commit. Only then can you replicate the experiemnts used in the paper. 

Note that if you are planning to add new functionality to our framework, you can work directly with the master branch and ignore the other branches. In this section, we are only describing how to replicate the experiments that we performed in our paper and the branches were constructed only for this purpose (for others to replicate our results).

Varying the weights for the weighted approach
---------------
Checkout the branch : weighted_vs_accuracy

Point HEAD to tag : expt_wa

Run (prep dataset) :
```
mvn -Dtest=DatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Change lines 115-117 in the file below to adjust the weights :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```

Run (weighted expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealWeighted test > /dev/null & disown
```

Data matching similarity threshold vs accuracy
---------------
Checkout the branch : similarity_vs_accuracy

Point HEAD to tag : expt_sa

Run (prep dataset) :
```
mvn -Dtest=DatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Change line 164 in the file below to adjust the thresholds :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```

Run (weighted expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealWeightedSim test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsFlexiSim test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsDynSim test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsLexSim test > /dev/null & disown
```

Theshold vs accuracy (constrained and lexical approaches)
---------------
Checkout the branch : constrained_and_lexical_vs_accuracy

Point HEAD to tag : expt_cla

Run (prep dataset) :
```
mvn -Dtest=DatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Change line 157 in the file below to adjust the thresholds :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```

Run (constrained expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsEVA test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsLexEVA test > /dev/null & disown
```

Error rate vs accuracy
---------------
Checkout the branch : errrate_vs_accuracy_and_numtups_vs_accuracy

Point HEAD to tag : expt_eata

Run (prep dataset) :
```
mvn -Dtest=DatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Run (weighted expt) :
```
mvn -Dtest=QRepairServiceTests#testSimulAnnealWeighted test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsDynamic test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsFlexi test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsLex test > /dev/null & disown
```

Number of tuples vs accuracy
---------------
Checkout the branch : errrate_vs_accuracy_and_numtups_vs_accuracy

Point HEAD to tag : expt_eata

Run (prep all IMDB datasets) :
```
mvn -Dtest=DatasetServiceTests#testPrepareIMDBAllDatasets test > /dev/null & disown
```

Uncomment line 53 and comment lines 54-55 in the following file :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```


Run (weighted expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealWeightedIMDB test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsDynamicIMDB test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsFlexiIMDB test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsLexIMDB test > /dev/null & disown
```

Threshold vs privacy loss (constrained and lexical approaches)
---------------
Checkout the branch : constrained_and_lexical_vs_pvtloss

Point HEAD to tag : expt_clp

Run (prep dataset) :
```
mvn -Dtest=DatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Change line 157 in the file below to adjust the thresholds :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```

Run (constrained expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsEVP test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=RepairServiceTests#testSimulAnnealEpsLexEVP test > /dev/null & disown
```

Error rate vs time taken
---------------
Checkout the branch : errrate_vs_time

Point HEAD to tag : expt_et

Run (prep dataset) :
```
mvn -Dtest=PDatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Run (weighted expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealWeighted test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsDynamic test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsFlexi test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsLex test > /dev/null & disown
```

Number of tuples vs time taken
---------------
Checkout the branch : numtups_vs_time

Point HEAD to tag : expt_tt

Run (prep all books datasets) :
```
mvn -Dtest=PDatasetServiceTests#testPrepareBooksAllDatasets test > /dev/null & disown
```

Run (weighted expt) :
```
mvn -Dtest=PRepairServiceTests#testNumTupsWeightedBooks test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=PRepairServiceTests#testNumTupsEpsDynamicBooks test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=PRepairServiceTests#testNumTupsEpsFlexiBooks test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=PRepairServiceTests#testNumTupsEpsLexBooks test > /dev/null & disown
```

Number of FDs vs time taken
---------------
Checkout the branch : numfds_vs_time

Point HEAD to tag : expt_ft

Uncomment lines 73-79 in the file below depending on the number of FDs :
```
vim src/test/java/data/cleaning/core/service/dataset/PDatasetServiceTests.java
```

Run (prep dataset) :
```
mvn -Dtest=PDatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Change line 97 in the file below to change the FD file :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```

Run (weighted expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealWeighted test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsDynamic test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsFlexi test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsLex test > /dev/null & disown
```

Note that you need to repeat all the steps (changing configs and preparing dataset) if you change the FD file.

Data matching similarity threshold vs time taken
---------------
Checkout the branch : similarity_vs_time

Point HEAD to tag : expt_st

Run (prep dataset) :
```
mvn -Dtest=PDatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Change line 99 in the file below to change similarity thresholds :
```
vim src/main/java/data/cleaning/core/utils/Config.java
```

Run (weighted expt) :
```
mvn -Dtest=PRepairServiceTests#testSimilaritySimulAnnealWeighted test > /dev/null & disown
```

Run (dynamic expt) :
```
mvn -Dtest=PRepairServiceTests#testSimilarityRateSimulAnnealEpsDynamic test > /dev/null & disown
```

Run (constrained expt) :
```
mvn -Dtest=PRepairServiceTests#testSimilaritySimulAnnealEpsFlexi test > /dev/null & disown
```

Run (lexical expt) :
```
mvn -Dtest=PRepairServiceTests#testSimilaritySimulAnnealEpsLex test > /dev/null & disown
```

Comparison experiments
---------------
Checkout the branch : bourgain_vs_sparsemap

Point HEAD to tag : expt_bs

Run (prep dataset) :
```
mvn -Dtest=QDatasetServiceTests#testPrepareDatasets test > /dev/null & disown
```

Run (weighted expt) :
```
mvn -Dtest=QRepairServiceTests#testMatchQuality test > /dev/null & disown
```

------------------------------------------------------------------------
6. Output and logging
------------------------------------------------------------------------
All logging and printing is controlled by log4j. This is useful for printing lengthy debugging output to a single log file from any Java class. 

The config file can be found in: src/main/resources/log4j.properties

Output for all experiments will be stored in the file : pvt_cleaning.out
