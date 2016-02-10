# Hadoop-Exercices
All sources code is based on the official training

###How to launch: averagewordlength
mvn clean install
mvn compile
mvn exec:java -Dexec.mainClass="org.apache.hadoop.averagewordlength.Main"

###How to launch: worcount
mvn clean install
mvn compile
mvn exec:java -Dexec.mainClass="org.apache.hadoop.wordcount.WordCount"

###How to launch: log_file_analysis
mvn clean install
mvn compile
mvn exec:java -Dexec.mainClass="org.apache.hadoop.log_file_analysis.ProcessLogs"