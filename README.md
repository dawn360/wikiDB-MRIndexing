Indexing Wiki Dumps
This indexing algorithm is a variation of Dean & Ghemawat Index algorithm

Run on Hadoop or CDH Cluster 
Project contains a pre-complied Jar

To complie
javac -classpath `hadoop classpath` *.java

Create MR Jar
jar cvf <JarName>.jar *.class

RUN MR JOB
hadoop jar <JarName>.jar GDIndex enwiki.xml <outputDir> <min_word_length(number>

View Results
hadoop fs -cat <outputDir>/part-r-* | less
