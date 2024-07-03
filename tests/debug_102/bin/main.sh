#!/bin/bash
# df vs spacesavers

DT=$(date +%Y%m%d)
THREADS="8"
OUTFILEGROUP="CCBR_Pipeliner"
KEEPCATALOG="1"
QUOTA=200

# setup header for df.tsv
df /data/CCBR |\
  awk -v dir="$dir" 'NR==1{$(NF+1)="proj_dir"} NR>1{$(NF+1)=dir}1' |\
  sed -E 's/Mounted on/Mounted_on/' |\
  sed -E 's/ +/\t/g' |\
  head -n 1 > df.tsv

# test each project dir
# CCBR projects 1332, 783, 984
for proj in 1332 783 984; do
    # audit permissions with find
    dir=/data/CCBR/projects/ccbr${proj}
    stdout=log/find.${proj}.out
    stderr=log/find.${proj}.err
    find ${dir} -type f ! -perm -g=r -or -type d ! -perm -g=r > ${stdout} 2> ${stderr}

    # check disk usage with df
    df ${dir} |\
        awk -v dir="${dir}" 'NR==1{$(NF+1)="proj_dir"} NR>1{$(NF+1)=dir}1' |\
        sed -E 's/ +/\t/g' |\
        tail -n 1 >> df.tsv
    
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
