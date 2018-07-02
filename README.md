# AWS2Hive
AWS to Hive data pipeline tools leveraging Apache Spark in Python

---

# Files included

### Runner script

**Script file:** run.py

**Description:** Handles arguments, creates contexts and runs the ETL process.

**Parameters list:**
- Bucket name
- Bucket prefix
- Dataset save directory
- Output Hive table name

**Sample run command:** *python run.py some_bucket some_prefix data_dir aws_data_table > run_result.txt*

---

### ETL script

**Script file:** aws2hive/etl.py

**Description:** Fetches data from Amazon S3 and loads it to Hive.
