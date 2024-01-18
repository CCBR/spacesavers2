# :rocket: spacesavers2 :rocket:

## Background

`spacesavers2`

- crawls through the provided folder (and its subfolders),
- gathers stats for each file like size, inode, user/group information, etc.,
- calculates unique hashes for each file,
- using the information gathers determines "duplicates",
- reports "high-value" duplicates, i.e., the ones that will give back most diskspace, if deleted,and
- makes a "counts-matrix" style matrix with folders as rownames and users a columnnames with each cell representing duplicate bytes.

> New improved parallel implementation of [`spacesavers`](https://github.com/CCBR/spacesavers). `spacesavers` is soon to be decommissioned!

> Note: `spacesavers2` requires [python version 3.11](https://www.python.org/downloads/release/python-3110/) or later and the [xxhash](https://pypi.org/project/xxhash/) library. These dependencies are already installed on biowulf (as a conda env). The environment for running `spacesavers2` can get set up using:
>
> ```bash
> . "/data/CCBR_Pipeliner/db/PipeDB/Conda/etc/profile.d/conda.sh" && \
> conda activate py311
> ```

## Commands

`spacesavers2` has the following Basic commands:

- [spacesavers2_catalog](catalog.md)
- [spacesavers2_mimeo](mimeo.md)
- [spacesavers2_grubbers](grubbers.md)
- [spacesavers2_blamematrix](blamematrix.md)
- [spacesavers2_usurp](usurp.md)
- [spacesavers2_e2e](e2e.md)

## Use case

One would like to monitor the per-user digital footprint on shared data drives like `/data/CCBR` on biowulf. Setting the `spacesavers2_e2e` as a weekly cronjob will allow automation of this task. `slurm_job` script is also provided to work as a template for using the job scheduler on the HPC to submit (possibly, as cronjob).

View the latest report [here](report.html).
