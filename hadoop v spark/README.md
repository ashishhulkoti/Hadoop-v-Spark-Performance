Make sure you have installed all the necessary packages. You can refer scripts/setup.sh to install hadoop and spark

To build the necessary java jar files run do the following. This will generate the jar file used for submitting mapreduce job
-> mvn package

To submit and run HadoopSort
->hadoop jar HadoopSort.jar HadoopSort /user/root/input /user/root/bin_files/output <number of records per mapper> <number of mappers>

To submit and run SparkSort
->spark-submit SparkSort.py

To verify bin file generated in HDFS
->python3 hdfs_hashverify.py <you hdfs path to bin file to be verified> -d true
