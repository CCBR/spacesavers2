#!/bin/bash
# df vs spacesavers

DT=$(date +%Y%m%d)
THREADS="8"
OUTFILEGROUP="CCBR_Pipeliner"
KEEPCATALOG="1"
QUOTA=200

# check df vs du entire /data/CCBR folder
du -s /data/CCBR
# this returns zero??
# 0       /data/CCBR
df /data/CCBR
df -h /data/CCBR

# check usage of each project directory with du
echo -e "usage\tproj_dir" > du.tsv
du -s /data/CCBR/projects/ccbr{1332,783,984} >> du.tsv

# run spacesavers on each project dir
# CCBR projects 1332, 783, 984
for proj in 1332 783 984; do
    # audit permissions with find
    dir=/data/CCBR/projects/ccbr${proj}
    stdout=log/find.${proj}.out
    stderr=log/find.${proj}.err
    find ${dir} -type f ! -perm -g=r -or -type d ! -perm -g=r > ${stdout} 2> ${stderr}

    # setup spacesavers variables
    OUTDIR="/home/sovacoolkl/data/debug-spacesavers/spacesavers/ccbr${proj}_${DT}"
    mkdir -p $OUTDIR
    FOLDER=$dir
    SUFFIX=$(echo $FOLDER | sed "s/\//_/g")

    echo "submitting spacesavers job for project $proj"
    # arguments to run_spacesavers2.sh
    # FOLDER=$1
    # OUTDIR=$2
    # QUOTA=$3
    # SUFFIX=$4
    # DT=$5
    # THREADS=$6
    # OUTFILEGROUP=$7
    # KEEPCATALOG=$8 1 for TRUE
    sbatch \
        --chdir=${OUTDIR} \
        --output="${SUFFIX}.spacesavers2.%j.out" \
        --error="${SUFFIX}.spacesavers2.%j.err" \
        --job-name="spacesavers2_${proj}" \
        --mail-type=BEGIN,END \
        --mem=120g \
        --partition="ccr,norm" \
        --time=24:00:00 \
        --cpus-per-task=8 \
        --wrap="bash /data/CCBR_Pipeliner/cronjobs/scripts/spacesavers2/run_spacesavers2.sh $FOLDER $OUTDIR $QUOTA $SUFFIX $DT $THREADS $OUTFILEGROUP $KEEPCATALOG"

done
