#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

# CONFIGURABLE

#readonly COMPRESSOR="gzip -f -k"
#readonly EXT="gz"

readonly COMPRESSOR="lzma --memlimit-compress=1GiB --best --compress --force --keep"
readonly EXT="lzma"


# Tool options
readonly PACKER=raw
readonly MIN_DURATION=5
readonly RECORD_DURATION=10
readonly RECORD_SIZE=50
readonly DEFAULT_TAIL_LINES=20
readonly PRIMARY_OPTS="--record-cluster 300 --record-pg-dump 1800"

readonly ALL_NODES="ceph01 ceph02 ceph03"
#readonly ALL_NODES="osd001 osd002 osd003 osd004 osd005 osd006 osd007 osd008 osd009 osd010 osd011 osd012"
#ALL_NODES1="ceph01 ceph02 ceph03 ceph04 ceph05 ceph06 ceph07 ceph08 ceph09 ceph10"
#ALL_NODES1="$ALL_NODES1 ceph11 ceph12 ceph13 ceph14 ceph15 ceph16 ceph17 ceph18 ceph19"
#ALL_NODES1="$ALL_NODES1 ceph20 ceph21 ceph22 ceph23 ceph24 ceph25 ceph26 ceph27 ceph28 ceph29"
#readonly ALL_NODES="$ALL_NODES1 ceph30 ceph31 ceph32 ceph33 ceph34 ceph35 ceph36 ceph38 ceph39 ceph40 ceph41 ceph42"

# ALMOST CONSTANT
readonly PYTHON="/usr/bin/python3.5"
readonly BIN=ceph_ho_dumper_async.py
readonly TARGET="/tmp/${BIN}"
readonly SHELL="/bin/bash"
readonly LOG=/tmp/ceph_ho_dumper.log
readonly RESULT=/tmp/historic_ops_dump.bin
readonly SRV_FILE=mira-ceph-ho-dumper.service
readonly FORCE_DUMP_SIGNAL=SIGUSR1
readonly MAX_LOG_UPDATE_WAIT_TIME=60


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
readonly MIN_DUMP_SIZE_DIFF=$((64 * 1024))

readonly _RES_DIR=$(dirname "${RESULT}")
readonly _RES_BASE=$(basename "${RESULT}")
readonly COPY_RESULT="${_RES_DIR}/copy_${_RES_BASE}"

readonly SRV_FILE_DST_PATH="/lib/systemd/system/${SRV_FILE}"
readonly DEFAULT_JOBS=$(echo "${ALL_NODES}" | wc --words)


function split_array {
    local -r data="${1}"
    local -r jobs="${2}"
    local -r count="$(echo "${data}" | wc --words)"

    local max_per_job=$((count / jobs))
    if (( max_per_job * jobs != count )); then
        max_per_job=$((max_per_job + 1))
    fi

    local curr_data=""
    local curr_idx=0
    local first=1
    for item in ${data} ; do
        curr_data="${curr_data} ${item}"
        curr_idx=$((curr_idx + 1))
        if (("${curr_idx}" >= "${max_per_job}")) ; then
            if [[ "${first}" == "0" ]] ; then
                echo -n "|"
            fi
            first="0"
            echo -n "${curr_data}"
            curr_data=""
            curr_idx="0"
        fi
    done

    if [[ "${curr_data}" != "" ]] ; then
        if [[ "${first}" == "0" ]] ; then
            echo -n "|"
        fi
        echo -n "${curr_data}"
    fi
}


function run_parallel {
    local -r jobs="${1}"
    local -r func="${2}"
    local -r nodes="${3}"

    shift
    shift
    shift

    IFS='|' read -r -a nodes_arr <<< $(split_array "${nodes}" "${jobs}")
    for curr_nodes in "${nodes_arr[@]}"; do
        ${func} "${curr_nodes}" "$@" &
    done
    wait
}


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

    local -r node="${1}"
    local -r cmd="${2}"

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
    local -r node="${1}"
    local -r cmd="${2}"
    do_ssh --sudo "${node}" "${cmd}"
}

function do_ssh_out {
    local -r node="${1}"
    local -r cmd="${2}"
    do_ssh --silent "${node}" "${cmd}"
}

function do_scp {
    local -r source="${1}"
    local -r target="${2}"
    echo "${source} => ${target}"
    ${SCP_CMD} "${source}" "${target}"
}

function clean {
    local -r nodes="${1}"
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
    local -r nodes="${1}"
    local cmd
    for node in ${nodes} ; do
        do_scp "${BIN}" "${node}:${TARGET}"
        cmd="systemctl stop ${SERVICE} || true && chown ${TARGET_USER_GRP} '${TARGET}' && "
        cmd+="chmod ${FILE_MODE} '${TARGET}' && systemctl start ${SERVICE}"
        do_ssh_sudo "${node}" "${cmd}"
    done
}

function deploy {
    local -r nodes="${1}"
    local -r first_node="${2}"
    local cmd
    local srv_file
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
                       --expression "s/{PY_FILE}/${TARGET//\//\\/}/" \
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


function get_record_file_size {
    local -r node="${1}"

    log_file_ll=$(do_ssh_out "${node}" "ls -l '${RESULT}' 2>&1 || true")

    if [[ "${log_file_ll}" != *"No such file or directory"* ]] ; then
        echo "${log_file_ll}" | awk '{print $5}'
    fi
}

function show {
    local -r nodes="${1}"
    local node
    local srv_stat
    local log_file_ll
    local log_size
    local pid


    for node in ${nodes} ; do
        srv_stat=$(do_ssh_out "${node}" "systemctl status ${SERVICE} | grep Active || true")


        log_file_ll=$(get_record_file_size "${node}")

        if [[ "${log_file_ll}" == "" ]] ; then
            log_size="NO FILE"
        else
            log_size=$(numfmt --to=iec-i --suffix=B "${log_file_ll}")
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
    done
}


function stop {
    local -r nodes="${1}"
    for node in ${nodes} ; do
        do_ssh_sudo "${node}" "systemctl stop ${SERVICE}"
    done
}

function tails {
    local -r nodes="${1}"
    local -r tail_lines="${2}"
    for node in ${nodes} ; do
        echo "${node}"
        do_ssh "${node}" "tail --lines ${tail_lines} '${LOG}'"
        echo
    done
}

function start {
    local -r nodes="${1}"
    for node in ${nodes} ; do
        do_ssh_sudo "${node}" "systemctl start ${SERVICE}"
    done
}

function restart {
    local -r nodes="${1}"
    for node in ${nodes} ; do
        do_ssh_sudo "${node}" "systemctl restart ${SERVICE}"
    done
}

function force_dump_info {
    local -r node="${1}"
    local -r wait="${2}"
    local new_size
    local origin_size

    if [[ "${wait}" == "1" ]] ; then
        origin_size=$(get_record_file_size "${node}")
        if [[ "${origin_size}" == "" ]] ; then
            origin_size=0
        fi
    fi
    do_ssh_sudo "${node}" "systemctl kill -s ${FORCE_DUMP_SIGNAL} ${SERVICE}"

    if [[ "${wait}" == "1" ]] ; then
        echo "Waiting for record file to be updated on node ${node}"
        local -r end_time=$(($(date +%s) + MAX_LOG_UPDATE_WAIT_TIME))
        while [[ "$(date +%s)" < "${end_time}" ]] ; do
            new_size=$(get_record_file_size "${node}")
            if ((origin_size + MIN_DUMP_SIZE_DIFF < new_size)) ; then
                break
            fi
            sleep 5
        done
    fi
}

function collect {
    local -r nodes="${1}"
    local -r target_dir="${2}"
    local -r result_basename="$(basename "${RESULT}")"

    pushd "${target_dir}" >/dev/null
    for node in ${nodes} ; do
        do_ssh "${node}" "${COMPRESSOR} '${RESULT}'"
        do_scp "${node}:${RESULT}.${EXT}" "${node}-${result_basename}.${EXT}"
        do_ssh "${node}" "rm --force '${RESULT}.${EXT}'"
    done
    popd  >/dev/null
}


function redeploy {
    local -r nodes="${1}"
    local -r first_node="${2}"
    clean "${nodes}"
    deploy "${nodes}" "${first_node}"
    start "${nodes}"
}

function main {
    local tgt=""
    local jobs="${DEFAULT_JOBS}"
    local tail_lines="${DEFAULT_TAIL_LINES}"
    local parallel="0"
    local parallel_prefix=""
    local no_dump_info="0"
    local wait="0"

    local -r command="${1}"
    shift

    while [[ "$#" != "0" ]] ; do
        case "${1}" in
        --jobs)
            jobs="${2}"
            shift
            ;;
        --target)
            tgt="${2}"
            shift
            ;;
        --lines)
            tail_lines="${2}"
            shift
            ;;
        --parallel|-P)
            parallel="1"
            ;;
        --no-dump-info)
            no_dump_info="1"
            ;;
        --wait)
            wait="1"
            ;;
        esac
        shift
    done

    if ! [[ "${jobs}" =~ ^[0-9]+$ ]] ; then
        echo "Incorrect job count: '${jobs}'" 1>&2
        exit 1
    fi

    if ! [[ "${tail_lines}" =~ ^[0-9]+$ ]] ; then
        echo "Incorrect tail lines count: '${tail_lines}'" 1>&2
        exit 1
    fi

    if [[ "${parallel}" == "1" ]] ; then
        parallel_prefix="run_parallel ${jobs}"
    fi

    local -r first_node=$(echo "${ALL_NODES}" | awk '{print $1}')

    case "${command}" in
    -c|--clean)
        ${parallel_prefix} clean "${ALL_NODES}"
        ;;
    -u|--update)
        ${parallel_prefix} update_bin "${ALL_NODES}"
        ;;
    -l|--collect)
        if [[ "${no_dump_info}" == "0" ]] ; then
            force_dump_info "${first_node}" "1"
        fi

        if [[ "${tgt}" != "" ]] && [[ ! -d "${tgt}" ]] ; then
            mkdir --parents "${tgt}"
        fi
        ${parallel_prefix} collect "${ALL_NODES}" "${tgt}"
        ;;
    -r|--redeploy)
        ${parallel_prefix} redeploy "${ALL_NODES}" "${first_node}"
        ${parallel_prefix} show "${ALL_NODES}" | sort
        ;;
    -d|--deploy)
        ${parallel_prefix} deploy "${ALL_NODES}" "${first_node}"
        ;;
    -s|--show)
        if [[ "${parallel}" == "1" ]] ; then
            ${parallel_prefix} show "${ALL_NODES}" | sort
        else
            show "${ALL_NODES}"
        fi
        ;;
    -f|--force-dump-info)
        force_dump_info "${first_node}" "${wait}"
        ;;
    -t|--tail)
        if [[ "${parallel}" == "1" ]] ; then
            echo "-t/--tail does not support parallel execution" 1>&2
            exit 1
        fi
        tails "${ALL_NODES}" "${tail_lines}"
        shift
        ;;
    -S|--start)
        ${parallel_prefix} start "${ALL_NODES}"
        ;;
    -T|--stop)
        ${parallel_prefix} stop "${ALL_NODES}"
        ;;
    -R|--restart)
        ${parallel_prefix} restart "${ALL_NODES}"
        ;;
    *)
        echo "Incorrect option provided ${command}" 1>&2
        exit 1
        ;;
    esac
}

(
    main "$@"
)

readonly exit_code="$?"

if [[ "${exit_code}" != 0 ]] ; then
    printf "%bFAILED!%b\n" "${RED}" "${NO_COLOR}" 2>&1
    exit "${exit_code}"
fi
