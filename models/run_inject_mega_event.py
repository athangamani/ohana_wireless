cde spark submit   --num-executors=4   \
 --executor-memory=4G   --executor-cores=4   \
 --driver-memory=4G  --conf "spark.kubernetes.memoryOverheadFactor=0.4"   \
 --conf "spark.kerberos.access.hadoopFileSystems=s3a://ps-amer-ohana-telecom"   \
 --conf "spark.hadoop.fs.s3a.endpoint.region=us-east-2"   \
 --conf "spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com"   \
 --packages "com.databricks:spark-xml_2.12:0.17.0"   \
 --python-env-resource-name pm-pandas-env  \
 inject_mega_event.py