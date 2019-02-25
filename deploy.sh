#!/usr/bin/env bash
set -o nounset

# CONFIGURABLE

#readonly COMPRESSOR="gzip -f -k"
#readonly EXT="gz"

readonly COMPRESSOR="lzma --memlimit-compress=1GiB --best --compress --force --keep"
readonly PRIMARY_OPTS="--record-cluster 300 --record-pg-dump 1800"
readonly EXT="lzma"


# Tool cli options
readonly PACKER=raw
readonly MIN_DURATION=100
readonly RECORD_DURATION=5
readonly RECORD_SIZE=100


readonly ALL_NODES="ceph01 ceph02 ceph03"
#ALL_NODES1="ceph01 ceph02 ceph03 ceph04 ceph05 ceph06 ceph07 ceph08 ceph09 ceph10"
#ALL_NODES1="$ALL_NODES1 ceph11 ceph12 ceph13 ceph14 ceph15 ceph16 ceph17 ceph18 ceph19"
#ALL_NODES1="$ALL_NODES1 ceph20 ceph21 ceph22 ceph23 ceph24 ceph25 ceph26 ceph27 ceph28 ceph29"
#readonly ALL_NODES="$ALL_NODES1 ceph30 ceph31 ceph32 ceph33 ceph34 ceph35 ceph36 ceph38 ceph39 ceph40 ceph41 ceph42"

# ALMOST CONSTANT
readonly PYTHON="/usr/bin/python3.5"
readonly BIN=ceph_ho_dumper.py
readonly TARGET="/tmp/${BIN}"
readonly SHELL="/bin/bash"
readonly LOG=/tmp/ceph_ho_dumper.log
readonly RESULT=/tmp/historic_ops_dump.bin
readonly SRV_FILE=mira-ceph-ho-dumper.service

# CONSTANTS
readonly TARGET_USER="ceph"
readonly TARGET_USER_GRP="${TARGET_USER}.${TARGET_USER}"
readonly FILE_MODE=644
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly NO_COLOR='\033[0m'

readonly DEFAULT_COUNT=20
readonly DEFAULT_DURATION=600
readonly SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectionAttempts=1 -o ConnectTimeout=5 -o LogLevel=ERROR"
readonly SSH_CMD="ssh ${SSH_OPTS} "
readonly SCP_CMD="scp ${SSH_OPTS} "
readonly SERVICE="${SRV_FILE}"

readonly _RES_DIR=$(dirname "${RESULT}")
readonly _RES_BASE=$(basename "${RESULT}")
readonly COPY_RESULT="${_RES_DIR}/copy_${_RES_BASE}"

readonly SRV_FILE_DST_PATH="/lib/systemd/system/${SRV_FILE}"
readonly DEFAULT_JOBS=$(echo "${ALL_NODES}" | wc --words)


function do_ssh {
    local run_shell="sudo su ${TARGET_USER} -s '${SHELL}'"
    local use_sudo=0
    local silent=0
    local lint_fake

    if [[ "${1}" == "--silent" ]] ; then
        silent=1
        lint_fake="${3}"
        shift
    fi

    if [[ "${1}" == "--sudo" ]] ; then
        run_shell="sudo bash"
        use_sudo=1
        lint_fake="${3}"
        shift
    fi

    local node="${1}"
    local cmd="${2}"

    if [[ "${silent}" == "0" ]] ; then
        if [[ "${use_sudo}" == "1" ]] ; then
            echo "${node}: SUDO: ${cmd}"
        else
            echo "${node}: ${cmd}"
        fi
    fi

    echo "${cmd}" | ${SSH_CMD} "${node}" -- "${run_shell}"
}

function do_ssh_sudo {
    do_ssh --sudo "${1}" "${2}"
}

function do_ssh_out {
    do_ssh --silent "${1}" "${2}"
}

function do_scp {
    local source="${1}"
    local target="${2}"
    echo "${source} => ${target}"
    ${SCP_CMD} "${source}" "${target}"
}

function clean {
    local nodes="${1}"
    local cmd
    local code=0
    local set_code
    local file_present

    for node in ${nodes} ; do
        cmd="systemctl stop ${SERVICE} ; rm --force '${SRV_FILE_DST_PATH}' ; systemctl daemon-reload || true"
        do_ssh_sudo "${node}" "${cmd}"

        is_active=$(do_ssh_out "${node}" "systemctl is-active --quiet '${SERVICE}' ; echo $?")
        if [[ "${is_active}" != "0" ]] ; then
            printf "%bFailed to stop service on node %s%b\n" "${RED}" "${node}" "${NO_COLOR}"
            code=1
        fi

        file_present=$(do_ssh_out "${node}" "ls '${TARGET}' 2>&1 || true")
        if [[ "${file_present}" != *"No such file or directory"* ]] ; then
            cmd="'${PYTHON}' '${TARGET}' set --duration ${DEFAULT_DURATION} --size ${DEFAULT_COUNT} >/dev/null 2>&1"
            set +e
            do_ssh "${node}" "${cmd}"
            set_code="$?"
            set -e
            if [[ "${set_code}" != "0" ]] ; then
                printf "%bFailed to set duration for node %s%b\n" "${RED}" "${node}" "${NO_COLOR}"
                code="${set_code}"
            fi
        fi

        do_ssh "${node}" "rm --force '${TARGET}' '${LOG}' '${RESULT}' || true"
    done
    if [[ "$code" != "0" ]] ; then
        exit "${code}"
    fi
}

function update_bin {
    local nodes="${1}"
    local cmd
    for node in ${nodes} ; do
        do_scp "${BIN}" "${node}:${TARGET}"
        cmd="systemctl stop ${SERVICE} || true && chown ${TARGET_USER_GRP} '${TARGET}' && "
        cmd+="chmod ${FILE_MODE} '${TARGET}' && systemctl start ${SERVICE}"
        do_ssh_sudo "${node}" "${cmd}"
    done
}

function deploy {
    local nodes="${1}"
    local cmd
    local srv_file
    local first_node=$(echo "${nodes}" | awk '{print $1}')
    local popt

    for node in ${nodes} ; do
        do_scp "${BIN}" "${node}:${TARGET}"

        if [[ "${node}" == "${first_node}" ]] ; then
            popt="${PRIMARY_OPTS}"
        else
            popt=""
        fi

        srv_file=$(sed --expression "s/{DURATION}/${RECORD_DURATION}/" \
                       --expression "s/{SIZE}/${RECORD_SIZE}/" \
                       --expression "s/{PACKER}/${PACKER}/" \
                       --expression "s/{MIN_DURATION}/${MIN_DURATION}/" \
                       --expression "s/{PRIMARY}/${popt}/" \
                       --expression "s/{PYTHON}/${PYTHON//\//\\/}/" \
                       --expression "s/{LOG_FILE}/${LOG//\//\\/}/" \
                       --expression "s/{USER}/${TARGET_USER}/" \
                       --expression "s/{RESULT}/${RESULT//\//\\/}/" < "${SRV_FILE}")

        echo "${srv_file}" | ${SSH_CMD} "${node}" "cat > /tmp/${SRV_FILE}"

        cmd="chown ${TARGET_USER_GRP} '${TARGET}' && "
        cmd+="chmod ${FILE_MODE} '${TARGET}' && "
        cmd+="mv '/tmp/${SRV_FILE}' '${SRV_FILE_DST_PATH}' && "
        cmd+="chown root.root '${SRV_FILE_DST_PATH}' && "
        cmd+="chmod 644 '${SRV_FILE_DST_PATH}' && "
        cmd+="systemctl daemon-reload"

        do_ssh_sudo "${node}" "${cmd}"
    done
}

function show_one_node {
    local node="${1}"
    local srv_stat
    local log_file_ll
    local log_size
    local pid

    srv_stat=$(do_ssh_out "${node}" "systemctl status ${SERVICE} | grep Active || true")
    log_file_ll=$(do_ssh_out "${node}" "ls -l '${RESULT}' 2>&1 || true")

    if [[ "${log_file_ll}" == *"No such file or directory"* ]] ; then
        log_size="NO FILE"
    else
        log_size=$(echo "${log_file_ll}" | awk '{print $5}' | numfmt --to=iec-i --suffix=B)
    fi

    pid=$(do_ssh_out "${node}" "systemctl --property=MainPID show ${SERVICE}")

    if [[ "${srv_stat}" == *" inactive "* ]] ; then
        printf "%-20s : %b %s %b data_sz = %s\n" "${node}" "${RED}" "${srv_stat}" "${NO_COLOR}" "${log_size}"
    else
        if [[ "${srv_stat}" == *" failed "* ]] ; then
            printf "%-20s : %b %s %b data_sz = %s\n" "${node}" "${RED}" "${srv_stat}" "${NO_COLOR}" "${log_size}"
        else
            printf "%-20s : %b %s %b pid = %s  data_sz = %s\n" \
                   "${node}" "${GREEN}" "${srv_stat}" "${NO_COLOR}" "${pid:8}" "${log_size}"
        fi
    fi
}

function show {
    local nodes="${1}"

    for node in ${nodes} ; do
        show_one_node "${node}" &
    done | sort
}

function stop {
    local nodes="${1}"
    for node in ${nodes} ; do
        do_ssh_sudo "${node}" "systemctl stop ${SERVICE}"
    done
}

function tails {
    local nodes="${1}"
    local tsize="${2}"
    for node in ${nodes} ; do
        echo "${node}"
        do_ssh "${node}" -- "tail --lines ${tsize} '${LOG}'"
        echo
    done
}

function start {
    local nodes="${1}"
    for node in ${nodes} ; do
        do_ssh_sudo "${node}" "systemctl start ${SERVICE}"
    done
}

function collect_one {
    local node="${1}"
    local rbn

    rbn="$(basename "${RESULT}")"
    do_ssh "${node}" "${COMPRESSOR} '${RESULT}'"
    do_scp "${node}:${RESULT}.${EXT}" "${node}-${rbn}.${EXT}"
    do_ssh "${node}" "rm --force '${RESULT}.${EXT}'"
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


function split_array {
    local data="${1}"
    local len=count

    count="$(echo "${data}" | wc --words)"

    14616627272574

    local max_nodes_per_job=$((nodes_count / jobs))
    if (( max_nodes_per_job * jobs != nodes_count )); then
        max_nodes_per_job=$((max_nodes_per_job + 1))
    fi
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
        start "${ALL_NODES}"
        show "${ALL_NODES}"
        ;;
    -d) deploy "${ALL_NODES}"
        start "${ALL_NODES}"
        ;;
    -D) deploy "${ALL_NODES}"
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

(
    set -o pipefail
    set -o errexit
    main "$@"
)

readonly exit_code="$?"

if [[ "${exit_code}" != 0 ]] ; then
    printf "%bFAILED!%b\n" "${RED}" "${NO_COLOR}" 2>&1
    exit "${exit_code}"
fi
