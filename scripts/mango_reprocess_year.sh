#!/usr/bin/env bash
# mango_reprocess_year.sh — reprocess all MANGO data for a year (XG and XR)
#
# Usage:
#   ./mango_reprocess_year.sh YEAR [N_PARALLEL]
#
#   YEAR        4-digit year to reprocess
#   N_PARALLEL  max concurrent jobs (default: 4)
#               Each instance uses up to 16 cores for the FIR filter step
#               (NUM_PROCESSORS in prepare_agimages.py), so 4 jobs fills a
#               64-core machine at peak.  Reduce on smaller machines.
#
# Priority:
#   Every job runs at nice 15 (low CPU priority) and ionice class 2 level 7
#   (best-effort low I/O priority).  Interactive workloads always preempt
#   these jobs; the batch resumes automatically when cores are free.
#
# Logs:
#   logs/mango_YEAR_TAG_DOY.log  — stdout/stderr per job (kept on failure)
#   mango_reprocess_YEAR.log     — one-line start/done/FAILED per job

set -uo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
YEAR=${1:?"Usage: $0 YEAR [N_PARALLEL]"}
N_PARALLEL=${2:-4}

CONDA_SH=/home/airglow/miniconda3/etc/profile.d/conda.sh
CONDA_ENV=airglowrsss
PYTHON=/home/airglow/miniconda3/envs/${CONDA_ENV}/bin/python
SCRIPT=/home/airglow/src/airglowrsss/src/airglow/MANGODisplay_OSN.py
export PYTHONPATH=/home/airglow/src/airglowrsss/src/

NORM=per_frame          # normalization mode
ENV_FILE=.env           # credentials file (relative to cwd, or absolute path)
# ──────────────────────────────────────────────────────────────────────────────

# Activate conda environment
# shellcheck source=/dev/null
source "${CONDA_SH}"
conda activate "${CONDA_ENV}"

# Determine number of days in YEAR (leap-year aware)
if (( YEAR % 4 == 0 && ( YEAR % 100 != 0 || YEAR % 400 == 0 ) )); then
    N_DAYS=366
else
    N_DAYS=365
fi

LOG_DIR=logs
mkdir -p "${LOG_DIR}"
RUN_LOG="mango_reprocess_${YEAR}.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S')  $*" | tee -a "${RUN_LOG}"; }

log "Starting reprocess: YEAR=${YEAR}  N_DAYS=${N_DAYS}  N_PARALLEL=${N_PARALLEL}  NORM=${NORM}"

# ── Per-job function ───────────────────────────────────────────────────────────
# Runs one (tag, doy) pair at reduced priority.  Keeps the per-job log on
# failure so you can inspect what went wrong.
run_one() {
    local tag=$1 doy=$2
    local doy3
    doy3=$(printf '%03d' "${doy}")
    local job_log="${LOG_DIR}/mango_${YEAR}_${tag}_${doy3}.log"

    log "START  ${tag}  DOY=${doy3}"
    if nice -n 15 ionice -c 2 -n 7 \
            "${PYTHON}" -u "${SCRIPT}" \
            -t "${tag}" -n "${NORM}" -y "${YEAR}" -d "${doy}" \
            -e "${ENV_FILE}" \
            >"${job_log}" 2>&1
    then
        log "DONE   ${tag}  DOY=${doy3}"
        rm -f "${job_log}"          # tidy up on success
    else
        log "FAILED ${tag}  DOY=${doy3}  (see ${job_log})"
    fi
}

export -f run_one
export PYTHON SCRIPT NORM ENV_FILE YEAR LOG_DIR RUN_LOG

# ── Job pool ───────────────────────────────────────────────────────────────────
# Interleave XG and XR for the same DOY so both tags benefit from the same
# download window.  At most N_PARALLEL jobs run at any one time.
# 'wait -n' (bash 4.3+) blocks until the next background job exits.
n_running=0

for doy in $(seq 1 "${N_DAYS}"); do
    for tag in XG XR; do
        run_one "${tag}" "${doy}" &
        n_running=$((n_running + 1))
        if (( n_running >= N_PARALLEL )); then
            wait -n
            n_running=$((n_running - 1))
        fi
    done
done

# Drain remaining jobs
wait
log "All done: YEAR=${YEAR}"

# ── Summary ───────────────────────────────────────────────────────────────────
n_failed=$(grep -c '^.*FAILED' "${RUN_LOG}" 2>/dev/null || true)
n_done=$(grep -c '^.*DONE' "${RUN_LOG}" 2>/dev/null || true)
log "Summary: ${n_done} succeeded, ${n_failed} failed"
if (( n_failed > 0 )); then
    log "Failed jobs:"
    grep 'FAILED' "${RUN_LOG}" | tee -a "${RUN_LOG}"
    exit 1
fi
