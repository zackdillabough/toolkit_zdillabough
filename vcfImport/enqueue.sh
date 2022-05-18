#!/usr/bin/env bash

##SBATCH -p compute-isilon
###SBATCH --job-name="SpliceVarianceMMRF"
##SBATCH -N 4                      # number of nodes
###SBATCH --time=0-0:02:00
###SBATCH --mail-user=zdillabough@tgen.org
##SBATCH -n 8                      # number of cores
##SBATCH --mem 2000

module load python/3.6.0

python vcfImport.py /home/zdillabough/testing/vcf_dummies
