# toolkit_zdillabough
This repository contains scripts from my 2018 Helios Scholars internship with the Translational Genomics Research Institute.

## vcfImport

This script annotates, formats, then imports an entire directory of .vcf files to TGen's NoSQL MongoDB by-patient by-gene.

### Usage

The script accepts only one required argument ```vcf_directory```, which is a directory containing .vcf (Variant Call Format) files.

Example: ```$ python vcfImport.py /path/to/vcfFiles/```

### Functionality

The script iterates through each .vcf file in ```vcf_directory```, where each file holds all variant information of a single tumor sample. Each file is annotated with SnpSift, and is then output to a .csv file. From here, the .csv file is read and converted into a hierarchical dict structure that is then imported to mongoDB.

The resulting database collection is composed of documents indexed by sample-id, and each document holds all single nucleotide variants (mutations) for each gene.

### Result

With this tool, a novel splicing event causing gene damage was found in the TRAF-3 tumor suppressor gene. This tool's approach has proven useful in precision medicine with not only multiple myeloma, but all genetic diseases.

## gtfImport

This script is used to import a GTF file to mongoDB.

## Exon_Analysis

This script is used to perform some basic analysis on exon length in a given database.
