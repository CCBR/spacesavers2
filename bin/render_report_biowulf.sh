#!/usr/bin/env bash
# to be executed from /data/CCBR_Pipeliner/Tools/spacesavers2/report
# Usage: bash bin/render_report_biowulf.sh
module load singularity
SINGULARITY_CACHEDIR=/data/CCBR_Pipeliner/SIFS

today=$(date +'%Y-%m-%d')
year=$(date +'%Y')
mkdir -p datashare/$year
html_filename="datashare/${year}/spacesavers2-report_${today}.html"
recipient_email="kelly.sovacool@nih.gov,vishal.koparde@nih.gov"

# TODO switch this to ~CCBR_Pipeliner after we create the datashare there
url=https://hpc.nih.gov/~sovacoolkl/spacesavers2/${year}/spacesavers2-report_${today}.html

echo "cd /mnt && \
    Rscript bin/render.R && \
    cp datashare/report.html $html_filename && \
    python src/send_email.py \
        $html_filename \
        $url \
        $recipient_email \
    " |\
    singularity exec -C -B $PWD:/mnt,/data/CCBR_Pipeliner/userdata/spacesavers2/:/mnt/data docker://nciccbr/spacesavers2:0.1.1 bash

cp -r datashare/* /data/sovacoolkl/datashare/spacesavers2/