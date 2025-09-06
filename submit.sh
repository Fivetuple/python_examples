#!/bin/bash

#SBATCH --time=48:00:00
#SBATCH --job-name=single_core
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=8G
#SBATCH --partition=medium
#SBATCH --clusters=all

#SBATCH --job-name=wikitree
#SBATCH --mail-type=END
#SBATCH --mail-user=paul.moore@spi.ox.ac.uk


module load Anaconda3
source activate $DATA/cwik

python wikitree_probate_match.py


