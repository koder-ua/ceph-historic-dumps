#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

# CONFIGURABLE

# Tool options
readonly PACKER=compact
readonly MIN_DURATION=10
readonly RECORD_DURATION=60
readonly RECORD_SIZE=300
readonly DEFAULT_TAIL_LINES=20
readonly PRIMARY_OPTS="--record-cluster 300 --record-pg-dump 1800"
readonly DUMP_HEADERS=""
readonly LOG_LEVEL="INFO"
readonly STATUS_CONN_PORT=16677
readonly STATUS_CONN_ADDR="0.0.0.0:${STATUS_CONN_PORT}"

readonly TEST_MIN_DURATION=5
readonly TEST_RECORD_DURATION=10
readonly TEST_RECORD_SIZE=300
readonly TEST_PRIMARY_OPTS="--record-cluster 30 --record-pg-dump 60"
readonly TEST_DUMP_HEADERS="--dump-unparsed-headers"
readonly TEST_LOG_LEVEL="DEBUG"

# ALMOST CONSTANT
readonly MAX_COMMUNICATION_TIMEOUT=15
readonly PYTHON="/usr/bin/python3.5"
readonly BIN="collector.py"
readonly BIN_PATH="collect_historic_ops/${BIN}"
readonly TARGET="/tmp/${BIN}"
readonly SHELL="/bin/bash"
readonly LOG="/tmp/ceph_ho_dumper.log"
readonly RESULT="/tmp/historic_ops_dump.bin"
readonly SRV_FILE="mira-ceph-ho-dumper.service"
readonly FORCE_DUMP_SIGNAL="SIGUSR1"
readonly MAX_PG_WAIT_TIME="60"


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

function clean_output {
    local -r nodes="${1}"
    for node in ${nodes} ; do
        do_ssh "${node}" "rm --force '${LOG}' '${RESULT}' || true"
    done
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
            do_ssh "${node}" "rm --force '${TARGET}' '${LOG}' '${RESULT}' || true"
        fi
    done

    if [[ "$code" != "0" ]] ; then
        exit "${code}"
    fi
}

function update_code {
    local -r nodes="${1}"
    local cmd
    for node in ${nodes} ; do
        do_scp "${BIN_PATH}" "${node}:${TARGET}"
        cmd="systemctl stop ${SERVICE} || true && chown ${TARGET_USER_GRP} '${TARGET}' && "
        cmd+="chmod ${FILE_MODE} '${TARGET}' && systemctl start ${SERVICE}"
        do_ssh_sudo "${node}" "${cmd}"
    done
}

function fill_srv {
    local -r primary="${1}"
    local -r test="${2}"

    local size="${RECORD_SIZE}"
    local log_level="${LOG_LEVEL}"
    local record_duration="${RECORD_DURATION}"
    local min_duration="${MIN_DURATION}"

    if [[ "${test}" == "1" ]] ; then
        size="${TEST_RECORD_SIZE}"
        log_level="${TEST_LOG_LEVEL}"
        record_duration="${TEST_RECORD_DURATION}"
        min_duration="${TEST_MIN_DURATION}"
    fi

    local popt=""
    if [[ "${primary}" == "1" ]] ; then
        if [[ "${test}" == "1" ]] ; then
            popt="${TEST_PRIMARY_OPTS}"
        else
            popt="${PRIMARY_OPTS}"
        fi
    fi

    sed --expression "s/{DURATION}/${record_duration}/" \
         --expression "s/{SIZE}/${size}/" \
         --expression "s/{PACKER}/${PACKER}/" \
         --expression "s/{STATUS_CONN_ADDR}/${STATUS_CONN_ADDR}/" \
         --expression "s/{LOG_LEVEL}/${log_level}/" \
         --expression "s/{DUMP_UNKNOWN_HEADERS}/${DUMP_HEADERS}/" \
         --expression "s/{MIN_DURATION}/${min_duration}/" \
         --expression "s/{PRIMARY}/${popt}/" \
         --expression "s/{PYTHON}/${PYTHON//\//\\/}/" \
         --expression "s/{PY_FILE}/${TARGET//\//\\/}/" \
         --expression "s/{LOG_FILE}/${LOG//\//\\/}/" \
         --expression "s/{USER}/${TARGET_USER}/" \
         --expression "s/{RESULT}/${RESULT//\//\\/}/" < "${SRV_FILE}"
}

function deploy {
    local -r nodes="${1}"
    local -r first_node="${2}"
    local -r test="${3}"
    local cmd
    local srv_file
    local primary

    for node in ${nodes} ; do
        do_scp "${BIN_PATH}" "${node}:${TARGET}"

        if [[ "${node}" == "${first_node}" ]] ; then
            primary="1"
        else
            primary="0"
        fi

        fill_srv "${primary}" "${test}" | ${SSH_CMD} "${node}" "cat > /tmp/${SRV_FILE}"

        cmd="chown ${TARGET_USER_GRP} '${TARGET}' && "
        cmd+="chmod ${FILE_MODE} '${TARGET}' && "
        cmd+="mv '/tmp/${SRV_FILE}' '${SRV_FILE_DST_PATH}' && "
        cmd+="chown root.root '${SRV_FILE_DST_PATH}' && "
        cmd+="chmod 644 '${SRV_FILE_DST_PATH}' && "
        cmd+="systemctl daemon-reload"

        do_ssh_sudo "${node}" "${cmd}"
    done
}


function get_file_size {
    local -r node="${1}"
    local -r file="${2}"

    local -r file_ll=$(do_ssh_out "${node}" "ls -l '${file}' 2>&1 || true")

    if [[ "${file_ll}" != *"No such file or directory"* ]] ; then
        echo "${file_ll}" | awk '{print $5}'
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


        record_file_size=$(get_file_size "${node}" "${RESULT}")
        if [[ "${record_file_size}" == "" ]] ; then
            record_file_size="NO FILE"
        else
            record_file_size=$(numfmt --to=iec-i --suffix=B "${record_file_size}")
        fi

        log_file_size=$(get_file_size "${node}" "${LOG}")
        if [[ "${log_file_size}" == "" ]] ; then
            log_file_size="NO FILE"
        else
            log_file_size=$(numfmt --to=iec-i --suffix=B "${log_file_size}")
        fi

        pid=$(do_ssh_out "${node}" "systemctl --property=MainPID show ${SERVICE}")

        if [[ "${srv_stat}" == *" inactive "* ]] ; then
            printf "%-20s : %b %s %b data_sz = %10s  log_sz = %10s\n" \
                "${node}" "${RED}" "${srv_stat}" "${NO_COLOR}" "${record_file_size}" "${log_file_size}"
        else
            if [[ "${srv_stat}" == *" failed "* ]] ; then
                printf "%-20s : %b %s %b data_sz = %10s  log_sz = %10s\n" \
                    "${node}" "${RED}" "${srv_stat}" "${NO_COLOR}" "${record_file_size}" "${log_file_size}"
            else
                printf "%-20s : %b %s %b pid = %10s  data_sz = %10s  log_sz = %10s\n" \
                       "${node}" "${GREEN}" "${srv_stat}" "${NO_COLOR}" "${pid:8}" \
                       "${record_file_size}" "${log_file_size}"
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
    local curr_pg_dump_time
    local end_wait_time
    local new_size

    if [[ "${wait}" == "1" ]] ; then
        curr_pg_dump_time=$(get_pg_dump_time "${node}" "${STATUS_CONN_PORT}" "${MAX_COMMUNICATION_TIMEOUT}")
    fi

    do_ssh_sudo "${node}" "systemctl kill -s ${FORCE_DUMP_SIGNAL} ${SERVICE}"

    if [[ "${wait}" == "1" ]] ; then
        echo "Waiting for primary node(${node}) to complete PG dump, max ${MAX_PG_WAIT_TIME} seconds"
        end_wait_time=$(($(date +%s) + MAX_PG_WAIT_TIME))

        while (( $(date +%s) <= end_wait_time )) ; do
            new_size=$(get_pg_dump_time "${node}" "${STATUS_CONN_PORT}" "${MAX_COMMUNICATION_TIMEOUT}")
            if [[ "${curr_pg_dump_time}" != "${new_size}" ]] ; then
                break
            fi
            sleep 1
        done
    fi
}

function collect {
    local -r nodes="${1}"
    local -r target_dir="${2}"
    local -r collect_logs="${3}"

    local -r result_basename="$(basename "${RESULT}")"
    local -r log_basename="$(basename "${LOG}")"

    pushd "${target_dir}" >/dev/null
    for node in ${nodes} ; do
        do_scp "${node}:${RESULT}" "${node}-${result_basename}"
        if [[ "${collect_logs}" == "1" ]] ; then
            do_scp "${node}:${LOG}" "${node}-${log_basename}"
        fi
    done
    popd  >/dev/null
}


function redeploy {
    local -r nodes="${1}"
    local -r first_node="${2}"
    local -r test="${3}"
    clean "${nodes}"
    deploy "${nodes}" "${first_node}" "${test}"
    start "${nodes}"
}

function get_pg_dump_time {
    local -r node="${1}"
    local -r port="${2}"
    local -r max_timeout="${3}"
    curl --max-time "${max_timeout}" -s "http://${node}:${port}/status.txt" \
        | grep "last_handler_run_at::pg_dump " \
        | awk '{print $2}'
}

# ignore EOF code from 'read'
set +e
read -r -d '' HELP <<- EOM
    Control ceph latency record service
    USAGE bash deploy.sh COMMAND [OPTIONS] INVENTORY_FILE

    Commands:
    -c --clean             Stop and remove service along with all files, logs and results
    -u --update            Update ${BIN} and restart service
    -l --collect           Collect results to current or specified folder
    -r --redeploy          Clean and redeploy system on and restart service
    -d --deploy            Only copy files and register service
    -s --show              Show current service status and record/log file size
    -f --force-dump-info   Force primary node to dump all cluster info now
    -t --tail              Show tail from service logs
    -C --clean-output      Clean output files and restart service
    -S --start             Start service
    -T --stop              Stop service
    -R --restart           Restart service
    -p --pack              Pack collected to local dir records/logs to archive
    --unpack               Unpack archive, previously packed with -p/--pack
    -h --help              Get this help

    Params:
    --parallel -P          Run commands for nodes in parallel. By default task count equal to node count,
                           but can be set with --jobs option
    --jobs JOB_COUNT       Max job count for parallel execution
    --lines COUNT          Lines count for -t/--tail command
    --test                 Run in test mode (smaller timeouts for collectors, DEBUG log level)
    --target NAME          Set folder to collect data to
    --no-dump-info         Do not force to dump cluster info before collect (for -l/--collect only)
    --collect-logs         Collect logs as wel as records (for -l/--collect only)
    --wait-for-update      Wait for cluster to dump info (for -f/--force-dump-info only)

    INVENTORY_FILE  list of all target nodes, one by line
EOM
set -e


function main {
    local tgt=""
    local tail_lines="${DEFAULT_TAIL_LINES}"
    local parallel="0"
    local parallel_prefix=""
    local no_dump_info="0"
    local test="0"
    local collect_logs="0"
    local wait_for_update="0"

    if [[ "$#" == "0" ]] ; then
        >&2 echo "${HELP}"
        exit 1
    fi

    local -r command="${1}"
    shift

    if [[ "${command}" == "-h" ]] || [[ "${command}" == "--help" ]] ; then
        echo "${HELP}"
        exit 0
    fi

    case "${command}" in
    -p|--pack)
        local -r result_file="last.tar"
        local -r log_arch=logs.tar.bz2
        tar cvjf "${log_arch}" *.log
        tar cvf "${result_file}" "${log_arch}" *.bin
        rm -f "${log_arch}"
        exit 0
        ;;
    --unpack)
        local -r result_file="last.tar"
        local -r log_arch=logs.tar.bz2
        tar xvf "${result_file}"
        tar xvjf "${log_arch}"
        rm -f "${log_arch}"
        exit 0
        ;;
    esac

    if [[ "$#" < "1" ]] ; then
        >&2 echo "${HELP}"
        exit 1
    fi


    local -r inventory_file="${@: -1}"
    if [[ ! -f "${inventory_file}" ]] ; then
        >&2 echo "No inventory file provided (or invalid provided)"
        exit 1
    fi

    local -r all_nodes=$(tr '\n' ' ' <"${inventory_file}")
    local jobs=$(echo "${all_nodes}" | wc --words)

    # parse options
    # skip last parameter - it is inventory
    while [[ "$#" != "1" ]] ; do
        case "${1}" in
        --jobs)
            jobs="${2}"
            shift
            ;;
        --lines)
            tail_lines="${2}"
            shift
            ;;
        --parallel|-P)
            parallel="1"
            ;;
        --test)
            test="1"
            ;;
        --target)
            tgt="${2}"
            shift
            ;;
        --no-dump-info)
            no_dump_info="1"
            ;;
        --collect-logs)
            collect_logs="1"
            ;;
        --wait-for-update)
            wait_for_update="1"
            ;;
        *)
            >&2 echo "Unknown extra option ${1}"
            exit 1
            ;;
        esac
        shift
    done

    if ! [[ "${jobs}" =~ ^[0-9]+$ ]] ; then
        >&2 echo "Incorrect job count: '${jobs}'"
        exit 1
    fi

    if ! [[ "${tail_lines}" =~ ^[0-9]+$ ]] ; then
        >&2 echo "Incorrect tail lines count: '${tail_lines}'"
        exit 1
    fi

    if [[ "${parallel}" == "1" ]] ; then
        parallel_prefix="run_parallel ${jobs}"
    fi

    local -r first_node=$(echo "${all_nodes}" | awk '{print $1}')

    case "${command}" in
    -c|--clean)
        ${parallel_prefix} clean "${all_nodes}"
        ;;
    -u|--update)
        ${parallel_prefix} update_code "${all_nodes}"
        ;;
    -l|--collect)
        if [[ "${no_dump_info}" == "0" ]] ; then
            force_dump_info "${first_node}" "1"
        fi

        if [[ "${tgt}" != "" ]] && [[ ! -d "${tgt}" ]] ; then
            mkdir --parents "${tgt}"
        fi
        ${parallel_prefix} collect "${all_nodes}" "${tgt}" "${collect_logs}"
        ;;
    -r|--redeploy)
        ${parallel_prefix} redeploy "${all_nodes}" "${first_node}" "${test}"
        ${parallel_prefix} show "${all_nodes}" | sort
        ;;
    -d|--deploy)
        ${parallel_prefix} deploy "${all_nodes}" "${first_node}" "${test}"
        ;;
    -s|--show)
        if [[ "${parallel}" == "1" ]] ; then
            ${parallel_prefix} show "${all_nodes}" | sort
        else
            show "${all_nodes}"
        fi
        ;;
    -f|--force-dump-info)
        force_dump_info "${first_node}" "${wait_for_update}"
        ;;
    -t|--tail)
        if [[ "${parallel}" == "1" ]] ; then
            echo "-t/--tail does not support parallel execution" 1>&2
            exit 1
        fi
        tails "${all_nodes}" "${tail_lines}"
        shift
        ;;
    -C|--clean-output)
        ${parallel_prefix} stop "${all_nodes}"
        ${parallel_prefix} clean_output "${all_nodes}"
        ${parallel_prefix} start "${all_nodes}"
        ;;
    -S|--start)
        ${parallel_prefix} start "${all_nodes}"
        ;;
    -T|--stop)
        ${parallel_prefix} stop "${all_nodes}"
        ;;
    -R|--restart)
        ${parallel_prefix} restart "${all_nodes}"
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
