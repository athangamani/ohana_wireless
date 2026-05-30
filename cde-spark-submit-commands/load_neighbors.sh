cde spark submit \
 --min-executors=4 \
 --max-executors=15 \
 --executor-memory=4G \
 --executor-cores=4 \
 --conf "spark.dynamicAllocation.initialExecutors=4" \
 --conf "spark.executor.instances=4" \
 --conf "spark.kerberos.access.hadoopFileSystems=s3a://ps-amer-ohana-telecom" \
 --conf "spark.hadoop.fs.s3a.endpoint.region=us-east-2" \
 --conf "spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com" \
 load_neighbors.py
