# toolkit_zdillabough
This repository contains scripts from my 2018 Helios Scholars internship with the Translational Genomics Research Institute.

## vcfImport

This script annotates, formats, then imports an entire directory of .vcf files to a MongoDB database by-sample by-gene. 

The purpose of this script is to give bioinformaticians the ability to convert massive amounts of genomic data into an easily queryable database. My personal research used this approach on Multiple Myeloma (MM) patient tumor samples to determine if non-damaging ("synonymous") splice-site mutations could actually cause damage to genes that play a key part in MM.

### Usage

The script accepts only one required argument ```vcf_directory```, which is a directory containing .vcf (Variant Call Format) files.

Example: ```$ python vcfImport.py /path/to/vcfFiles/```

### Functionality

The script iterates through each .vcf file in ```vcf_directory```, where each file holds all variant information of a single tumor sample. Each file is annotated with SnpSift, and is then output to a .csv file. From here, the .csv file is read and converted into a hierarchical dict structure that is then imported to mongoDB.

The resulting database collection is composed of documents indexed by sample-id, and each document holds all single nucleotide variants (mutations) for each gene.

### Result

With this tool, a novel splicing event causing genetic damage (intron retention) was found in the TRAF-3 tumor suppressor gene and has therefore proven useful in precision medicine. In addition, this approach has proven useful with not only multiple myeloma, but all genetic diseases.

## gtfImport

This script is used to import a GTF file to mongoDB.

## Exon_Analysis

This script is used to perform some basic analysis on exon length in a given database.
