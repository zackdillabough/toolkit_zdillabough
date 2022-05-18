"""
Description: This script is used to format and import GTF files.
             The GTF to be imported is stored in the 'GTFpath' variable in main().
"""
import os, sys, re, pdb
from pymongo import MongoClient


def main():
	# establish connection to mongodb
	os.system('mongod')
	client = MongoClient('mongodb://diva.tgen.org:27077')
	db = client['MMRF_IA13a']
	collection = db['Zack_GTF']
	
	# open GTF file and begin reading
	GTFpath = '/home/tgenref/homo_sapiens/grch37_hg19/hs37d5_tgen/gene_model/ensembl_v74/Homo_sapiens.GRCh37.74.gtf.hs37d5.EGFRvIII.gtf'
	gtf_file = open(GTFpath, 'r')
	currentLine = gtf_file.readline()
	bulk_recs = []

	while len(currentLine) != 0:
		count = int(0)

		# create list of records to bulkWrite to db (while currentline is not empty)
		while count < 100000:
			if len(currentLine) == 0:
					break
			bulk_recs.append(format_record(currentLine))
			currentLine = gtf_file.readline()
			count += 1
		collection.insert_many(bulk_recs)
		bulk_recs = []

	# put your shoes on and go home
	gtf_file.close()


def format_record(tab_line):
	# split the tab-delim line into a fields list
	fields = tab_line.split('\t')

	# use regular expressions to find sub-column/value pairs in the "Attributes" group 
	# and update the pairs to the "attributes" dict
	columnNames = re.findall('\w+(?=\s\")', fields[-1])
	values = re.findall('(?<=\")([\w\s\d]+.*?)(?=\")', fields[-1])
	attributes = dict()

	for x in range(0, len(values)):
		if(columnNames[x] == "exon_number"):
			attributes.update({columnNames[x] : int(values[x])})
		else:
			attributes.update({columnNames[x] : values[x]})

	record = { 
			'SEQNAME' : fields[0],
			'FEATURE' : fields[1],
			'REGION' : fields[2],
			'START' : int(fields[3]),
			'END' : int(fields[4]),
			'SCORE' : fields[5],
			'STRAND' : fields[6],
			'FRAME' : fields[7],
			'ATTRIBUTES' : attributes
			}

	return record

if __name__ == "__main__":
	main()
