#!/bin/bash
#PBS -q normalsl
#PBS -l ncpus=12
#PBS -l mem=72GB
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/xp65+gdata/access+gdata/ce10+scratch/ce10+gdata/gb02
#PBS -l wd
#PBS -l jobfs=200GB
#PBS -P ce10

module purge
module use /g/data/xp65/public/modules; module load conda/analysis3-25.08
module use /g/data/gb02/public/modules/;module load dask_setup

python /home/561/mjl561/git/HM_coupled/analysis/load_data.py \
    --pbs \
    --root /scratch/ce10/mjl561/outputs/u-dy159 \
    --model HM-AU_12_BOM_CLIM_GAL9 \
    --variable av_lat_hflx \
    --ncdir SLV1H


