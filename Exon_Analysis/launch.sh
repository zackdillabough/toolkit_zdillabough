#module load spark

srun --pty --mem 100G /home/gotero/spark/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --driver-memory 50G --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2  --executor-memory 50G exon_len.py

# run on diva cluster
# /packages/spark/spark-2.3.1-bin-hadoop2.7/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 exon_len.py

# run on dback2 cluster
# srun --pty --mem 100G /home/gotero/spark/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2  exon_len.py


# misc

# run pyspark w/ mongo connection '''
# pyspark --conf "spark.mongodb.input.uri=mongodb://diva.tgen.org:27077/MMRF_IA13a.SomaticVariants?readPreference=primaryPreferred" --conf "spark.mongodb.output.uri=mongodb://diva.tgen.org:27077/MMRF_IA13a.SomaticVariants" --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 --conf “spark.sql.autoBroadcastJoinThreshold=-1”

# for easy creation of a "sparksession" obj in pyspark console 
# my_spark = SparkSession.builder.appName("s2m").config("spark.mongodb.input.uri", "mongodb://diva.tgen.org:27077/MMRF_IA13a.SomaticVariants").config("spark.mongodb.output.uri", "mongodb://diva.tgen.org:27077/MMRF_IA13a.SomaticVariants").getOrCreate()
