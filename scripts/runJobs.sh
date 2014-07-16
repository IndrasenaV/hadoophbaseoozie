export HADOOP_CUSTOM_HOME = ''
export HBASE_CUSTOM_HOME = ''
export HADOOP_URL = ''
export JOB_JAR_LOCATION = ''
cd $HADOOP_CUSTOM_HOME/bin
hadoop jar $JOB_JAR_LOCATION com.webanalytics.mapreduce.DataRawLoad
hadoop jar $JOB_JAR_LOCATION com.webanalytics.mapreduce.daily.BrowserAccess
hadoop jar $JOB_JAR_LOCATION com.webanalytics.mapreduce.daily.LocationAnalyze
hadoop jar $JOB_JAR_LOCATION com.webanalytics.mapreduce.daily.OSAccess
hadoop jar $JOB_JAR_LOCATION com.webanalytics.mapreduce.daily.SiteVisit
