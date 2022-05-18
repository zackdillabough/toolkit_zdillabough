from pymongo import MongoClient
from bson import json_util
import re, os, sys, argparse, json, time, pdb
import pandas as pd


# adds a forward-slash to the end of a path if needed
def path_name_groom(path):
	if path[-1:] == "/":
		return path
	else:
		return path + "/"


def main():
	t2 = time.time()
	parser = argparse.ArgumentParser(description='Define a path containing VCFs that you\'d \
	like to process and import to mongoDB') 

	parser.add_argument("vcf_directory", help="Path to directory containing unfiltered .vcf files")
	
	args = parser.parse_args()

	vcfPath = path_name_groom(args.vcf_directory)
	newFile_num = 0

	
	# create connection to mongodb
	os.system('mongod')
	client = MongoClient('mongodb://diva.tgen.org:27077')
	db = client['MMRF_IA13a']
	collection = db['Zack_Variant_By_Gene']

	# List of runtimes (per variant import to mongo) :: for performance testing
	times = []
	
	# open the source file and file we'd like to write to; init variables
	for file in os.listdir(vcfPath):
		t0 = time.time()
		if not file.endswith(".vcf"): 
		    continue

		currentFile = vcfPath + file
		newFile_name = vcfPath + str(newFile_num) + ".vcf"

		newFile = open(newFile_name, 'w')
		sourceFile = open(currentFile, 'r')
		currentLine = sourceFile.readline()

		# traverse to first record; extract sample-id from header in process
		while currentLine[:1] == '#':
			counter = 0
			if "SnpEffCmd=" in currentLine:
				id_line = currentLine
				line_list = id_line.split(' ')
				u_score = "_"
				full_id = u_score.join(line_list[-2:-1])
				tumor_id = full_id.split('-')[1]
				sample_id = u_score.join(tumor_id.split('_')[:4])
			counter = counter + 1
			currentLine = sourceFile.readline()

		currentLine = sourceFile.readline()
		currentLine = sourceFile.readline()

		# read data for "CALLERS_COUNT=x". if CALLERS_COUNT >1,
		# write record to 'filtered.vcf' file
		while len(currentLine) != 0:
			reObject = re.search('(?<=CALLERS_COUNT=)\d', currentLine)
			numCallers = int(reObject.group(0))

			if numCallers > 1:
				newFile.write(currentLine)
				currentLine = sourceFile.readline()
			else:
				currentLine = sourceFile.readline()
		newFile_num = newFile_num + 1
		sourceFile.close()
		newFile.close()

		newFile_name_csv = vcfPath + file[:-4] + ".csv"

		snpEff_fields = "\"CHROM\"  \"POS\"	 \"ID\"	  \"REF\"	 \"ALT\"	 \"1000G\"   \"COSMIC\"  \"DB\"	  \"NHLBI\"   \"CALLERS_COUNT\"   \"SEURAT\"  \"MUTECT\"  \"STRELKA\" \"RNA_REF_COUNT\"   \"RNA_ALT_COUNT\"   \"RNA_ALT_FREQ\"	\"GEN[0].AD[0]\"	\"GEN[0].AD[1]\"	\"GEN[0].AR\"	   \"GEN[0].DP\"	\"GEN[1].AD[0]\"	\"GEN[1].AD[1]\"	\"GEN[1].AR\"	   \"GEN[1].DP\"	   \"dbNSFP_GERP___RS\"		\"dbNSFP_LRT_score\"		\"dbNSFP_MutationAssessor_score\"   \"dbNSFP_FATHMM_score\"	 \"dbNSFP_GERP___NR\"		\"dbNSFP_CADD_raw\" \"dbNSFP_MutationTaster_score\"	 \"dbNSFP_CADD_raw_rankscore\"	   \"dbNSFP_CADD_phred\"	   \"dbNSFP_MetaSVM_score\"	\"dbNSFP_MetaSVM_rankscore\"		\"dbNSFP_MetaSVM_pred\"	 \"dbNSFP_MetaLR_score\"	 \"dbNSFP_MetaLR_rankscore\" \"dbNSFP_MetaLR_pred\"	  \"dbNSFP_Reliability_index\"		\"dbNSFP_Interpro_domain\"  \"dbNSFP_Polyphen2_HDIV_score\"	 \"dbNSFP_SIFT_score\"	   \"dbNSFP_Polyphen2_HVAR_score\"	 \"dbNSFP_Polyphen2_HVAR_pred\"	  \"ANN[*]ALLELE\"	\"ANN[*]EFFECT\"   \"ANN[*]IMPACT\"   \"ANN[*]GENE\"	 \"ANN[*]GENEID\"   \"ANN[*]FEATURE\"  \"ANN[*]FEATUREID\"	\"ANN[*]BIOTYPE\"	\"ANN[*]RANK\"	\"ANN[*]HGVS_C\"   \"ANN[*]HGVS_P\"   \"ANN[*]CDNA_POS\" \"ANN[*]CDNA_LEN\" \"ANN[*]CDS_POS\"  \"ANN[*]CDS_LEN\"  \"ANN[*]AA_POS\"   \"ANN[*]AA_LEN\"   \"ANN[*]DISTANCE\" \"ANN[*]ERRORS\"   \"LOF[*].GENE\"	 \"LOF[*].GENEID\"   \"LOF[*].NUMTR\"	\"LOF[*].PERC\""

		os.system("java -jar /home/tgenref/binaries/snpEff/snpEff_v3.6c/snpEff/SnpSift.jar extractFields -s \",\" -e \".\" %s %s > %s" % (
		newFile_name, snpEff_fields, newFile_name_csv))
		os.system("rm %s" % (newFile_name))
	

		# create mongo document to upsert all records to (for current VCF file)
		doc = collection.insert_one({'Sample_ID' : sample_id}).inserted_id
		
		format_2_mongo(newFile_name_csv, doc, collection, sample_id)
		os.system('rm %s' % newFile_name_csv)
		t1 = time.time()
		times.append(t1 - t0)
	
	# output script performance
	t3 = time.time()
	avg_rt_pv = sum(times) / len(times)
	tot_runtime = (t3 - t2)
	print("Average import time (per variant): " + str(avg_rt_pv))
	print("\'s2m_gene\'s total runtime: " + str(tot_runtime))


def format_2_mongo(filename, doc_id, coll, s_id):
	readFile = open(filename, 'r')
	fileString = readFile.read()
	fileLines = fileString.split('\n')[1:]

	GEN_0 = []
	GEN_1 = []
	LOF = []
	INFO = []
	gene_count = dict()
	
	for line in fileLines[:-1]:
		fields = line.split('\t')
		INFO.extend(fields[0:16])
		INFO.extend(fields[24:45])
		GEN_0.extend(fields[16:20])
		GEN_1.extend(fields[20:24])
		ANNS = fields[45]
		LOF.extend(fields[-4:])
		
		ANN = ANNS.split("|")
		
		# have to do this, as there aren't enough fields being 'extracted' by snpsift (why??)
		while len(ANN) < 19:
			ANN.append("")
			
		SNV_rec = {
			'CHROM': INFO[0],
			'POS': INFO[1],
			'ID': INFO[2],
			'REF': INFO[3],
			'ALT': INFO[4],
			'1000G': INFO[5],
			'COSMIC': INFO[6],
			'DB': INFO[7],
			'NHLBI': INFO[8],
			'CALLERS_COUNT': INFO[9],
			'SEURAT': INFO[10],
			'MUTECT': INFO[11],
			'STRELKA': INFO[12],
			'RNA_REF_COUNT': INFO[13],
			'RNA_ALT_COUNT': INFO[14],
			'RNA_ALT_FREQ': INFO[15],
			'GEN[0].AD[0]': GEN_0[0],
			'GEN[0].AD[1]': GEN_0[1],
			'GEN[0].AR': GEN_0[2],
			'GEN[0].DP': GEN_0[3],
			'GEN[1].AD[0]': GEN_1[0],
			'GEN[1].AD[1]': GEN_1[1],
			'GEN[1].AR': GEN_1[2],
			'GEN[1].DP': GEN_1[3],
			'dbNSFP_GERP___RS': INFO[16],
			'dbNSFP_LRT_score': INFO[17],
			'dbNSFP_MutationAssessor_score': INFO[18],
			'dbNSFP_FATHMM_score': INFO[19],
			'dbNSFP_GERP___NR': INFO[20],
			'dbNSFP_CADD_raw': INFO[21],
			'dbNSFP_MutationTaster_score': INFO[22],
			'dbNSFP_CADD_raw_rankscore': INFO[23],
			'dbNSFP_CADD_phred': INFO[24],
			'dbNSFP_MetaSVM_score': INFO[25],
			'dbNSFP_MetaSVM_rankscore': INFO[26],
			'dbNSFP_MetaSVM_pred': INFO[27],
			'dbNSFP_MetaLR_score': INFO[28],
			'dbNSFP_MetaLR_rankscore': INFO[29],
			'dbNSFP_MetaLR_pred': INFO[30],
			'dbNSFP_Reliability_index': INFO[31],
			'dbNSFP_Interpro_domain': INFO[32],
			'dbNSFP_Polyphen2_HDIV_score': INFO[33],
			'dbNSFP_SIFT_score': INFO[34],
			'dbNSFP_Polyphen2_HVAR_score': INFO[35],
			'dbNSFP_Polyphen2_HVAR_pred': INFO[36],
			'ANN[*]': {'ALLELE': ANN[0],
				'EFFECT': ANN[1],
				'IMPACT': ANN[2],
				'GENE': ANN[3],
				'GENEID': ANN[4],
				'FEATURE': ANN[5],
				'FEATUREID': ANN[6],
				'BIOTYPE': ANN[7],
				'RANK': ANN[8],
				'HGVS_C': ANN[9],
				'HGVS_P': ANN[10],
				'CDNA_POS': ANN[11],
				'CDNA_LEN': ANN[12],
				'CDS_POS': ANN[13],
				'CDS_LEN': ANN[14],
				'AA_POS': ANN[15],
				'AA_LEN': ANN[16],
				'DISTANCE': ANN[17],
				'ERRORS': ANN[18]},
			'LOF[*]': {
				'GENE': LOF[0],
				'GENEID': LOF[1],
				'NUMTR': LOF[2],
				'PERC': LOF[3]}
			}

		record = {
			'sample' : s_id,
			'gene': ANN[3],
			'ENSG': LOF[1],
			'snvs': SNV_rec 
					}
		if ANN[3] in gene_count.keys():
			gene_count.update({ANN[3] : gene_count.get(ANN[3]) + 1})
		else:
			gene_count.update({ANN[3] : 1})

		numGene = "#" + str(gene_count.get(ANN[3]))
		coll.update({'_id' : doc_id}, {'$set': {ANN[3] + "." + numGene : record}}, upsert=True) 
	
	readFile.close()


if __name__ == '__main__':
	main()
