h1. Hadoop-Exercices
All sources code is based on the official training

h3. ow to launch: averagewordlength
@mvn clean install@
@mvn compile@
@mvn exec:java -Dexec.mainClass="org.apache.hadoop.averagewordlength.Main"@

h3. How to launch: worcount
@mvn clean install@
@mvn compile@
@mvn exec:java -Dexec.mainClass="org.apache.hadoop.wordcount.WordCount"@

h3. How to launch: log_file_analysis
@mvn clean install@
@mvn compile@
@mvn exec:java -Dexec.mainClass="org.apache.hadoop.log_file_analysis.ProcessLogs"@