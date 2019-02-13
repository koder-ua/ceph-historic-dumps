#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

# CONFIGURABLE

#COMPRESSOR="gzip -f -k"
#EXT="gz"

readonly COMPRESSOR="lzma -9 -z -k -f"
readonly EXT="lzma"

readonly RECORD_DURATION=60
readonly RECORD_SIZE=100

readonly ALL_NODES="ceph01 ceph02 ceph03"
#ALL_NODES1="ceph01 ceph02 ceph03 ceph04 ceph05 ceph06 ceph07 ceph08 ceph09 ceph10"
#ALL_NODES1="$ALL_NODES1 ceph11 ceph12 ceph13 ceph14 ceph15 ceph16 ceph17 ceph18 ceph19"
#ALL_NODES1="$ALL_NODES1 ceph20 ceph21 ceph22 ceph23 ceph24 ceph25 ceph26 ceph27 ceph28 ceph29"
#readonly ALL_NODES="$ALL_NODES1 ceph30 ceph31 ceph32 ceph33 ceph34 ceph35 ceph36 ceph38 ceph39 ceph40 ceph41 ceph42"

# ALMOST CONSTANT

readonly BIN=ceph_ho_dumper.py
readonly TARGET="/tmp/${BIN}"
readonly LOG=/tmp/ceph_ho_dumper.log
readonly RESULT=/tmp/historic_ops_dump.bin
readonly SRV_FILE=mira-ceph-ho-dumper.service

# CONSTANTS

readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly NC='\033[0m' # No Color

readonly DEFAULT_COUNT=20
readonly DEFAULT_DURATION=600
readonly SSH_CMD="ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
readonly SCP_CMD="scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
readonly SERVICE="${SRV_FILE}"
readonly SRV_FILE_DST_PATH="/lib/systemd/system/${SRV_FILE}"
readonly DEFAULT_JOBS=$(echo "${ALL_NODES}" | wc --words)


function do_ssh {
    local node="${1}"
    local cmd="${2}"
    set -x
    echo "${cmd}" | ${SSH_CMD} "${node}" -- sudo bash
    { set +x ; } 2>/dev/null
}

function do_ssh_out {
    local node="${1}"
    local cmd="${2}"
    echo "${cmd}" | ${SSH_CMD} "${node}" -- sudo bash
}

function do_scp {
    local source="${1}"
    local target="${2}"
    set -x
    ${SCP_CMD} "${source}" "${target}"
    { set +x ; } 2>/dev/null
}

function clean {
    local nodes="${1}"
    local cmd
    for node in ${nodes} ; do
        cmd="systemctl stop ${SERVICE} ; "
        cmd+="systemctl disable ${SERVICE} ; "
        cmd+="${TARGET} set --duration=${DEFAULT_DURATION} --count=${DEFAULT_COUNT} >/dev/null 2>&1 ; "
        cmd+="rm -f ${SRV_FILE_DST_PATH} ${TARGET} ${LOG} ${RESULT} || true"
        do_ssh "${node}" "${cmd}"
    done
}

function update_bin {
    local nodes="${1}"
    for node in ${nodes} ; do
        do_ssh "${node}" "sudo systemctl stop ${SERVICE}"
        do_scp "${BIN}" "${node}:${TARGET}"
        do_ssh "${node}" "chown root.root ${TARGET} && chmod +x ${TARGET} && sudo systemctl start ${SERVICE}"
    done
}

function deploy {
    local nodes="${1}"
    local cmd
    local srv_file
    for node in ${nodes} ; do
        do_scp "${BIN}" "${node}:${TARGET}"

        srv_file=$(sed --expression "s/{DURATION}/${RECORD_DURATION}/" \
                       --expression "s/{SIZE}/${RECORD_SIZE}/" \
                       --expression "s/{LOG_FILE}/${LOG//\//\\/}/" \
                       --expression "s/{RESULT}/${RESULT//\//\\/}/" < "${SRV_FILE}")

        echo "${srv_file}" | ${SSH_CMD} "${node}" "cat > /tmp/${SRV_FILE}"

        cmd="chown root.root ${TARGET} && "
        cmd+="chmod +x ${TARGET} && "
        cmd+="mv /tmp/${SRV_FILE} ${SRV_FILE_DST_PATH} &&"
        cmd+="chown root.root ${SRV_FILE_DST_PATH} && "
        cmd+="systemctl daemon-reload && "
        cmd+="systemctl enable ${SERVICE} && "
        cmd+="systemctl start ${SERVICE}"

        do_ssh "${node}" "${cmd}"
    done
}

function show {
    local nodes="${1}"
    local srv_stat
    local log_file_ll
    local log_size

    for node in ${nodes} ; do

        srv_stat=$(do_ssh_out "${node}" "sudo systemctl status ${SERVICE}" | grep Active || true)
        log_file_ll=$(do_ssh_out "${node}" "ls -l ${RESULT}" 2>&1 || true)

        if [[ "${log_file_ll}" == *"No such file or directory"* ]] ; then
            log_size="NO FILE"
        else
            log_size=$(echo "${log_file_ll}" | awk '{print $5}' | numfmt --to=iec-i --suffix=B)
        fi

        if [[ "${srv_stat}" == *" inactive "* ]] ; then
            printf "%-20s : %b %s %b data_sz = %s\n" "${node}" "${RED}" "${srv_stat}" "${NC}" "${log_size}"
        else
            if [[ "${srv_stat}" == *" failed "* ]] ; then
                printf "%-20s : %b %s %b data_sz = %s\n" "${node}" "${RED}" "${srv_stat}" "${NC}" "${log_size}"
            else
                printf "%-20s : %b %s %b data_sz = %s\n" "${node}" "${GREEN}" "${srv_stat}" "${NC}" "${log_size}"
            fi
        fi
    done
}

function stop {
    local nodes="${1}"
    for node in ${nodes} ; do
        do_ssh "${node}" "sudo systemctl stop ${SERVICE}"
    done
}

function tails {
    local nodes="${1}"
    local tsize="${2}"
    for node in ${nodes} ; do
        echo "${node}"
        $SSH_CMD "${node}" -- "tail --lines ${tsize} \"${LOG}\""
        echo
    done
}

function start {
    local nodes="${1}"
    for node in ${nodes} ; do
        do_ssh "${node}" "sudo systemctl start ${SERVICE}"
    done
}

function collect_one {
    local node="${1}"
    local rbn

    rbn="$(basename "${RESULT}")"
    do_ssh "${node}" "sudo ${COMPRESSOR} ${RESULT}"
    do_scp "${node}:${RESULT}.${EXT}" "${node}-${rbn}.${EXT}"
    do_ssh "${node}" "sudo rm --force ${RESULT}.${EXT}"
}

function collect {
    local nodes="${1}"
    local target_dir="${2}"

    pushd "${target_dir}" >/dev/null
    for node in ${nodes} ; do
        collect_one "${node}"
    done
    popd  >/dev/null
}

function collect_parallel {
    local nodes="${1}"
    local jobs="${2}"
    local tgt_node="${3}"
    local nodes_count
    nodes_count="$(echo "${nodes}" | wc --words)"

    local max_nodes_per_job=$((nodes_count / jobs))
    if (( max_nodes_per_job * jobs != nodes_count )); then
        max_nodes_per_job=$((max_nodes_per_job + 1))
    fi

    local curr_nodes=""
    local count=0
    for node in ${nodes} ; do
        curr_nodes="${curr_nodes} ${node}"
        count=$((count + 1))
        if (( count == max_nodes_per_job )) ; then
            collect "${curr_nodes}" "${tgt_node}" &
            curr_nodes=""
            count=0
        fi
    done

    if [[ "${curr_nodes}" != "" ]] ; then
        collect "${curr_nodes}" "${tgt_node}" &
    fi

    wait
}

function main {
    # parse/validate/prepare parameters for -l -L options
    local tgt
    local jobs

    if [[ "${1}" == "-L" ]] ; then
        case "$#" in
        1) tgt="."
           jobs="${DEFAULT_JOBS}"
           ;;
        2) tgt="${2}"
           jobs="${DEFAULT_JOBS}"
           ;;
        3) tgt="${2}"
           jobs="${3}"
           ;;
        *) echo "Incorrect options provided" 1>&2 && exit 1
           ;;
        esac

        if ! [[ "${jobs}" =~ ^[0-9]+$ ]] ; then
            echo "Incorrect job count: '${jobs}'" 1>&2
            exit 1
        fi
    fi

    if [[ "${1}" == "-l" ]] ; then
        case "$#" in
        1) tgt="."
           ;;
        2) tgt="${2}"
           ;;
        *) echo "Incorrect options provided" 1>&2 && exit 1
           ;;
        esac
    fi

    if [[ ("${1}" == "-l" || "${1}" == "-L") && "${tgt}" != "." && ! -d "${tgt}" ]] ; then
        mkdir --parents "${tgt}"
    fi

    # main

    case "${1}" in
    -c) clean "${ALL_NODES}"
        ;;
    -u) update_bin "${ALL_NODES}"
        ;;
    -l) collect "${ALL_NODES}" "${tgt}"
        ;;
    -L) collect_parallel "${ALL_NODES}" "${jobs}" "${tgt}"
        ;;
    -r) clean "${ALL_NODES}"
        deploy "${ALL_NODES}"
        show "${ALL_NODES}"
        ;;
    -d) deploy "${ALL_NODES}"
        ;;
    -s) show "${ALL_NODES}"
        ;;
    -t) local tsize
        if [[ "$#" != 2 ]] ; then
            tsize=10
        else
            tsize="${2}"
        fi

        if ! [[ "${tsize}" =~ ^[0-9]+$ ]] ; then
            echo "Incorrect tail size: '${tsize}'" 1>&2
            exit 1
        fi

        tails "${ALL_NODES}" "${tsize}"
        shift
        ;;
    -S) start "${ALL_NODES}"
        ;;
    -T) stop "${ALL_NODES}"
        ;;
    *)  echo "Incorrect options provided" 1>&2 && exit 1
        ;;
    esac
}

main "$@"
