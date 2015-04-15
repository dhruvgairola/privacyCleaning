------------------------------------------------------------------------
1. Introduction
------------------------------------------------------------------------
Data privacy with data cleaning code.


------------------------------------------------------------------------
2. Requirements for running project
------------------------------------------------------------------------
Java 7, Maven 3.0.3


------------------------------------------------------------------------
3. Running the project
------------------------------------------------------------------------
Existing datasets
-----------------------
Please download the files from here :
https://drive.google.com/file/d/0B6gIlEKnOlv1UHQxcUlLR25ncnc/view?usp=sharing

Unzip the files into the root directory.

New datasets
-----------------------
The first line of the csv file has to be the column names for both the target and master datasets.

FD file
-----------------------
If you have an FD of the form "Mobile Number, First Name -> Last Name", then convert this to csv "Mobile Number, First Name, Last Name". The last csv element is the consequent while everything before is the antecedent. Make sure that all the attributes match the column names defined in the dataset files.

Configuring project
-----------------------
```
vim src/main/java/data/cleaning/core/utils/Config.java
```
This is the configuration file. Add the config for the new datasets just as the current datasets are specified.

To run the tests
-----------------------
General advice
---------------
```
mvn clean install
```

Run above code everytime before running the tests (if code was changed since the last time). Note that the tests below don't account for the dataset preprocessing. That info will be added later.

Quality tests
---------------
Err rate vs accuracy :
```
mvn -Dtest=QDatasetServiceTests#testPrepareDatasets test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealWeighted test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsDynamic test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsFlexi test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsLex test > /dev/null & disown
```

Inc tuples vs accuracy :
```
mvn -Dtest=QDatasetServiceTests#testPrepareIMDBAllDatasets test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealWeightedIMDB test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsDynamicIMDB test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsFlexiIMDB test > /dev/null & disown
mvn -Dtest=QRepairServiceTests#testSimulAnnealEpsLexIMDB test > /dev/null & disown
```

Comparative tests (Bourgain)
---------------
```
mvn -Dtest=QRepairServiceTests#testMatchQuality test > /dev/null & disown
```

Performance tests
---------------
Create all books :
```
mvn -Dtest=PDatasetServiceTests#testIncRecsBooksDataset test > /dev/null & disown
```

Inc errs vs time :
```
mvn -Dtest=PDatasetServiceTests#testPrepareDatasets test > /dev/null & disown
mvn -Dtest=PDatasetServiceTests#testPrintStatsAboutErrors test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealWeighted test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsDynamic test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsFlexi test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testErrRateSimulAnnealEpsLex test > /dev/null & disown
```

Inc tuples vs time :
```
mvn -Dtest=PDatasetServiceTests#testPrepareBooksAllDatasets test > /dev/null & disown
mvn -Dtest=PDatasetServiceTests#testPrintStatsAboutAllBooksErrors test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testNumTupsWeightedBooks test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testNumTupsEpsDynamicBooks test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testNumTupsEpsFlexiBooks test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testNumTupsEpsLexBooks test > /dev/null & disown
```

Similarity vs time :
```
mvn -Dtest=PRepairServiceTests#testSimilaritySimulAnnealWeighted test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testSimilarityRateSimulAnnealEpsDynamic test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testSimilaritySimulAnnealEpsFlexi test > /dev/null & disown
mvn -Dtest=PRepairServiceTests#testSimilaritySimulAnnealEpsLex test > /dev/null & disown
```

Useful commands
-----------------------
```
ps -ef | grep java | awk '{print $2}' | xargs kill -9
```

To log println statements
-----------------------
All logging and printing is controlled by log4j. This is useful for printing lengthy debugging output to a single log file from any Java class. 

The config file can be found in: src/main/resources/log4j.properties

Change the config file to your liking by following the online documentation for log4j. By default, all logging of the form :
logger.log(SecurityLevel.SECURITY, "Foobar");
is printed to the console and also to a file called pvt_cleaning.out (the log file is not tracked on git).

------------------------------------------------------------------------
4. Replicating experiments
------------------------------------------------------------------------

To replicate each experiment, please check out the relevant branch. Each branch is tagged with the "expt_*", where "*" represents some unique name. Checkout the tagged commit in order to replicate the experiments.