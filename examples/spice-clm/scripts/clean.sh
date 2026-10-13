#!/bin/bash
# Delete the intermediate data of one month after post-processing (SPICE post clean-up).
# Usage: clean.sh <dir> ...
# Archive, time series, restart files and logs are kept.
set -eu
for DIR in "$@"; do
  [[ -d "${DIR}" ]] || { echo "ERROR: ${DIR} is not a directory" >&2; exit 1; }
done
for DIR in "$@"; do
  echo "[CLEAN] $(du -sh "${DIR}" | cut -f1) ${DIR}"
  rm -rf -- "${DIR}"
done
echo "[CLEAN] finished"
