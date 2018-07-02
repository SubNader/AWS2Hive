####################################################################################
##  Runner script | Handles arguments, creates contexts and runs the ETL process. ##
####################################################################################

import sys
from aws2hive import etl
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

if __name__ == '__main__':

	# Handle arguments
	bucket_name = sys.argv[1] 
	path_prefix = sys.argv[2]
	dataset_dir = sys.argv[3]
	hive_table_name = sys.argv[4]

	# Create Spark and Hive contexts
	conf = SparkConf().setAppName('AWS2Hive ETL').setMaster('local[*]')
	sc = SparkContext(conf=conf)
	hiveContext = HiveContext(sc)

	# Run the ETL process
	print('Starting the ETL process..\n')
	etl.run_etl(hiveContext, bucket_name, path_prefix, dataset_dir, hive_table_name)
	print('The ETL process has been completed.\n')