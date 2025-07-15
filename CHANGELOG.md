## spacesavers2 development version

- Add multiple versions for docs website. (#109, @kelly-sovacool)
- Now using the readthedocs theme for the docs website. (#117, @kelly-sovacool)
- Fix path to the shared conda environment on biowulf. (#120, @kelly-sovacool)

## spacesavers2 v0.14.0

- Fix accuracy of bytes calculation when hard links exist. (#106, @kopardev)

## spacesavers2 v0.13.0

### New features

- adding new commands `spacesavers2_pdq_create_db` and `spacesavers2_pdq_update_db`
- output TSV files from `spacesavers2_pdq` can be saved into a sqlite3 db with these commands
- future integration with Grafana will now be possible

## spacesavers2 v0.12.1

### New features

- adding uid, human_readable_bytes and percent columns to `pdq` output

### Bug fixes

- type fix "quite" to "quiet"

## spacesavers2 v0.12.0

### New features

- `spacesavers2_pdq` is now counting inodes (not files) and including links and directories (#95, @kopardev)
- "pathlib.glob" is replaced with "os.scandir" for speedy folder traversing
- `--quite` option added to `spacesavers2_pdq` and `spacesavers2_catalog` to suppress progress bar output when running non-interactively eg. as a cronjob. This reduces size of .err file.

### Bug fixes

- `spacesavers2_pdq` not does NOT ignore links and folders (#93, @kopardev)
- `redirect` correctly captures intermediate non-zero exit codes
- "eval" statements removed from `spacesavers2_e2e` to accurately capture non-zero exit codes; makes sure e2d fails if catalog fails internally

## spacesavers2 0.11.6

### New features

- Move the report to a separate internal repository (#79, @kelly-sovacool)
- new option `--json` for `spacesavers2_pdq`

### Bug fixes

- `redirect` script now checks if running on BIOWULF or FRCE. If not, then checks for python version and "xxhash" library (fix #91, @kopardev)

## spacesavers2 0.11.5

### New features

- new command `spacesavers2_pdq` to get per-user number of files and number of bytes

## spacesavers2 0.11.4

### New features

- `--geezers`, `--geezerage` and `--geezersize` arguments are added to `spacesavers2_catalog` to report really old files per user.
- documents updated

### Bug fixes

## spacesavers2 0.11.3

### New features

- brokenlinks are reported on a per user basis
- progress bar added to `spacesavers2_catalog` and `spacesavers2_mimeo`

## spacesavers2 0.11.2

### New features

- `spacesavers2_e2e` has a new `-v` or `--version` argument to print version of spacesavers2 being used

## spacesavers2 0.11.1

### New features

- `spacesavers2_e2e` now shows more defaults in "--help"

### Bug fixes

- grubbers do not include non-duplicate copies (aka hardlinks) (#78, @kopardev)
- blamematix reports numbers correctly (minor bug fix)

## spacesavers2 0.11.0

### New features

- Add `requirements.txt` for easy creation of environment in "spacesavers2" docker (#68, @kopardev)
- `grubbers` has new `--outfile` argument.
- `blamematrix` has now been moved into `mimeo`.
- `mimeo` files.gz always includes the original file as the first one in the filelist.
- `mimeo` now has kronatools compatible output. ktImportText is also run if in PATH to generate HTML report for duplicates only. (#46, @kopardev)
- Update documentation.

### Bug fixes

- `e2e` overhauled, improved and well commented.
- `grubbers` `--limit` can be < 1 GiB (float) (#70, @kopardev)
- `grubbers` output file format changed. New original file column added. Original file is required by `usurp`.
- `mimeo` `--duplicateonly` now correctly handles duplicates owned by different UIDs. (#71, @kopardev)
  - Update `blamematrix` and to account for corrected duplicate handling in `mimeo`.
- `usurp` now uses the new "original file" column from `grubbers` while creating hard-links.
- Total size now closely resembles `df` results (fix #75 @kopardev)
- Files with future timestamps are handled correctly (fix #76, @kopardev)

## spacesavers2 0.10.2

- Now tracking user-facing changes with a changelog. (#61, @kelly-sovacool)
- Generated a report of spacesavers metrics on a cron schedule and sent via email. (#36, #48, #60, #63, @kelly-sovacool)
- Print the version. (#40, #41, @kopardev)
- Bugfixes. (#56, @kopardev)
- fix `requirements.txt` to meet GH Dependabot (#66, @kopardev)
- Improvements to grubbers and usurp docs. (#57, @kopardev)

## spacesavers2 0.10.1

Release notes were not written for this version or prior.
