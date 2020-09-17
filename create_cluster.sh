#! bin/zsh
aws emr create-cluster \
--name data-lake-cluster \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark \
--bootstrap-actions Path=s3://s3-for-spark-cluster/bootstrap_emr.sh \
--ec2-attributes KeyName=spark-cluster \
--instance-type m5.xlarge \
--log-uri s3://s3-for-spark-cluster/
