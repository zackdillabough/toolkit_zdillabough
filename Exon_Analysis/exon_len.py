"""
Description: This script performs some statistics on the exon lengths of a given NoSQL database of VCF files.
             Database candidates are found in the 'Database selection' section.
"""
import pdb
from pyspark.sql import *

my_spark = SparkSession \
				.builder \
				.appName("s2m") \
			    .config("spark.mongodb.input.uri", "mongodb://diva.tgen.org:27077/MMRF_IA13a.Zack_GTF") \
   				.config("spark.mongodb.output.uri", "mongodb://diva.tgen.org:27077/MMRF_IA13a.Zack_GTF") \
				.getOrCreate()

## Database selection ##
# Zack_GTF collection
df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# AllVariants collection
# df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://diva.tgen.org:27077/MMRF_IA13a.AllVariants").load()

# SomaticVariants collection
# df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://diva.tgen.org:27077/MMRF_IA13a.SomaticVariants").load()

## Analysis section ##
# cast START STOP columns as integers, and create a new column "LENGTH" that displays the result of STOP-START
df = df.withColumn("START", df["START"].cast("int")).withColumn("END", df["END"].cast("int"))
df = df.withColumn("LENGTH", df["END"] - df["START"])

df_feature_filt = df.filter(df['FEATURE'] == 'protein_coding')
df_feature_filt = df_feature_filt.filter(df['LENGTH'] < 400)
df_feature_filt.show()

# display exon length stats (mean, med, mode, etc) 
df_feature_filt.describe(['LENGTH']).show()
