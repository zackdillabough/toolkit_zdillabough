#from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
import re, os, argparse, time, pdb
import pandas as pd
import collections


def main():
	t0 = time.time()

	parser = argparse.ArgumentParser(description='Define paths you\'d \
	like to process VCF files from and where you want to place them')

	parser.add_argument("vcf_directory", help="Path to directory containing unfiltered .vcf files")
	args = parser.parse_args()

	vcfPath = path_name_groom(args.vcf_directory)
	newFile_num = 0
	
	# establish spark connection
	my_spark = SparkSession \
					.builder \
					.appName("s2m") \
				    .config("spark.mongodb.input.uri", "mongodb://diva.tgen.org:27077/MMRF_IA13a.zack_test") \
    				.config("spark.mongodb.output.uri", "mongodb://diva.tgen.org:27077/MMRF_IA13a.zack_test") \
					.getOrCreate()

	import_times = []

	# open the source file and file we'd like to write to; init variables
	for file in os.listdir(vcfPath):
		t2 = time.time()
		if not file.endswith(".vcf"):
			continue
	
		# open the current file in specified dir
		current_filename = vcfPath + file
		sourceFile = open(current_filename, 'r')
		
		# traverse the vcf header and grab the sample_id
		currentLine = sourceFile.readline()
		while currentLine[:1] == '#':
			if "SnpEffCmd=" in currentLine:
				id_line = currentLine
				line_list = id_line.split(' ')
				u_score = "_"
				full_id = u_score.join(line_list[-2:-1])
				tumor_id = full_id.split('-')[1]
				sample_id = u_score.join(tumor_id.split('_')[:4])
				break
			currentLine = sourceFile.readline()
		
		sourceFile.close()
	
		temp_csv = vcfPath + file[:-4] + ".csv"

        # snpsift fields
		snpEff_fields = "\"CHROM\"  \"POS\"	 \"ID\"	  \"REF\"	 \"ALT\"	 \"1000G\"   \"COSMIC_C\"	\"COSMIC_NC\"	\"DB\"	  \"NHLBI\"   \"CALLERS_COUNT\"   \"SEURAT\"  \"MUTECT\"  \"STRELKA\" \"RNA_REF_COUNT\"   \"RNA_ALT_COUNT\"   \"RNA_ALT_FREQ\"	\"GEN[0].AD[0]\"	\"GEN[0].AD[1]\"	\"GEN[0].AR\"	   \"GEN[0].DP\"	\"GEN[1].AD[0]\"	\"GEN[1].AD[1]\"	\"GEN[1].AR\"	   \"GEN[1].DP\"	   \"dbNSFP_GERP___RS\"	\"dbNSFP_LRT_score\"	\"dbNSFP_MutationAssessor_score\"   \"dbNSFP_FATHMM_score\"	 \"dbNSFP_GERP___NR\"	\"dbNSFP_CADD_raw\" \"dbNSFP_MutationTaster_score\"	 \"dbNSFP_CADD_raw_rankscore\"	   \"dbNSFP_CADD_phred\"	   \"dbNSFP_MetaSVM_score\"	\"dbNSFP_MetaSVM_rankscore\"	\"dbNSFP_MetaSVM_pred\"	 \"dbNSFP_MetaLR_score\"	 \"dbNSFP_MetaLR_rankscore\" \"dbNSFP_MetaLR_pred\"	  \"dbNSFP_Reliability_index\"	\"dbNSFP_Interpro_domain\"  \"dbNSFP_Polyphen2_HDIV_score\"	 \"dbNSFP_SIFT_score\"	   \"dbNSFP_Polyphen2_HVAR_score\"	 \"dbNSFP_Polyphen2_HVAR_pred\"	  \"ANN[*].ALLELE\"	\"ANN[*].EFFECT\"   \"ANN[*].IMPACT\"   \"ANN[*].GENE\"	 \"ANN[*].GENEID\"   \"ANN[*].FEATURE\"  \"ANN[*].FEATUREID\"	\"ANN[*].BIOTYPE\"	\"ANN[*].RANK\"	\"ANN[*].HGVS_C\"   \"ANN[*].HGVS_P\"   \"ANN[*].CDNA_POS\" \"ANN[*].CDNA_LEN\" \"ANN[*].CDS_POS\"  \"ANN[*].CDS_LEN\"  \"ANN[*].AA_POS\"   \"ANN[*].AA_LEN\"   \"ANN[*].DISTANCE\" \"ANN[*].ERRORS\"   \"LOF[*].GENE\"	 \"LOF[*].GENEID\"   \"LOF[*].NUMTR\"	\"LOF[*].PERC\""

		os.system("cat %s | /home/zdillabough/snpEff/snpEff/scripts/vcfEffOnePerLine.pl | java -jar /home/zdillabough/snpEff/snpEff/SnpSift.jar extractFields -s \",\" -e \".\" - %s > %s" % (current_filename, snpEff_fields, temp_csv))
		#os.system("java -jar /home/zdillabough/snpEff/snpEff/SnpSift.jar extractFields -s \",\" -e \".\" %s %s > %s" % (current_filename, snpEff_fields, temp_csv))	

		# send the csv off to mongo
		importToDB(temp_csv, my_spark, sample_id)

		# delete temp .csv file 
		os.system('rm %s' % (temp_csv))
		t3 = time.time()
		import_times.append(t3 - t2)

	# Performance testing (report written to stdout)
	t1 = time.time()
	print("Total runtime: " + str(t1 - t0))
	print("Average file-import time: " + str(sum(import_times) / len(import_times)))

''' This method imports a patient-sample csv  
	and imports the data to a mongoDB collection'''
def importToDB(filename, spark, s_id):
	readFile = open(filename, 'r')
	fileString = readFile.read()
	fileLines = fileString.split('\n')
	columnNames = fileLines[0].split('\t')
	dataLines = fileLines[1:]
	
	# remove "." from column names
	for x in range(0, len(columnNames)):
			if "." in columnNames[x]:
					columnNames[x] = columnNames[x].replace(".", "")

	# this will store every gene's "record" + list of SNVs in the file
	bulk_recs = dict()
	
	# iterate through each line of the specified "readFile"
	for line in dataLines[:-1]:
		fields = line.split('\t')
		snv = makehash()
		for x in range(0, len(columnNames)):
				snv[columnNames[x]] = fields[x]
		
		# the "makehash" dict won't allow "." in keys
		if "." in snv['ANN[*]GENE']:
			gene_name = str(snv['ANN[*]GENE'].replace(".", ""))
		else:
			gene_name = str(snv['ANN[*]GENE'])

		# Create a gene-name keyed entry in "bulk_recs" (if one doesn't already exist).
		# This allows for appending variant-data ( key: "snvs" ) to the correct gene's data.
		if gene_name not in bulk_recs:
			header = makehash()
			snv_list = []
			header = {
			'sample': s_id,
			'gene': snv['ANN[*]GENE'],
			'effect': snv['ANN[*]EFFECT'],
			'ENSG': snv['ANN[*]GENEID'],
			'snvs': snv_list
			}
			bulk_recs[gene_name] = header
		else:
			bulk_recs[gene_name]['effect'] = bulk_recs[gene_name]['effect'] + "," + snv['ANN[*]EFFECT']
		
		bulk_recs[gene_name]['snvs'].append(snv)
	
	#pdb.set_trace()
	df = pd.DataFrame.from_dict(bulk_recs)
	json_df = df.to_json()
	spark_df = spark.createDataFrame(bulk_recs)
	df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
	df.show()

	#insert the entirety of bulk_recs to the collection!
	#for x in bulk_recs.keys():
	#	spark.insert_one(bulk_recs[x])

''' give a path a "/" if she needs one '''
def path_name_groom(path):
	if path[-1:] == "/":
		return path
	else:
		return path + "/"

''' hierarchical dict structure
	(regular dict wont allow for accessing nested dicts) '''
def makehash():
	return collections.defaultdict(makehash)


if __name__ == '__main__':
	main()
