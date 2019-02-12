#!/usr/bin/env bash
set -e -u -o pipefail -o errexit

# CONFIGURABLE

#COMPRESSOR="gzip -f -k"
#EXT="gz"

COMPRESSOR="lzma -9 -z -k -f"
EXT="lzma"

RECORD_DURATION=60
RECORD_SIZE=100

ALL_NODES="ceph01 ceph02 ceph03"
#ALL_NODES="ceph01 ceph02 ceph03 ceph04 ceph05 ceph06 ceph07 ceph08 ceph09 ceph10"
#ALL_NODES="$ALL_NODES ceph11 ceph12 ceph13 ceph14 ceph15 ceph16 ceph17 ceph18 ceph19"
#ALL_NODES="$ALL_NODES ceph20 ceph21 ceph22 ceph23 ceph24 ceph25 ceph26 ceph27 ceph28 ceph29"
#ALL_NODES="$ALL_NODES ceph30 ceph31 ceph32 ceph33 ceph34 ceph35 ceph36 ceph38 ceph39 ceph40 ceph41 ceph42"

# ALMOST CONSTANT

BIN=ceph_ho_dumper.py
TARGET="/tmp/$BIN"
LOG=/tmp/ceph_ho_dumper.log
RESULT=/tmp/historic_ops_dump.bin
SRV_FILE=mira-ceph-ho-dumper.service

# CONSTANTS

DEFAULT_COUNT=20
DEFAULT_DURATION=600
SSH_CMD="ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SCP_CMD="scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SERVICE="$SRV_FILE"
SRV_FILE_DST_PATH="/lib/systemd/system/$SRV_FILE"


function clean {
    NODES="$1"
    for NODE in $NODES ; do
        CMD="systemctl stop $SERVICE ; systemctl disable $SERVICE"
        CMD="$CMD ; $TARGET set --duration=$DEFAULT_DURATION --count=$DEFAULT_COUNT >/dev/null 2>&1 "
        CMD="$CMD ; rm -f $SRV_FILE_DST_PATH $TARGET $LOG $RESULT"
        set -x
        echo "$CMD" | $SSH_CMD "$NODE" sudo bash || true
        { set +x ; } 2>/dev/null
    done
}

function update_bin {
    NODES="$1"
    for NODE in $NODES ; do
        CMD="chown root.root $TARGET && chmod +x $TARGET && sudo systemctl start $SERVICE"
        set -x
        $SSH_CMD "$NODE" sudo systemctl stop "$SERVICE"
        $SCP_CMD "$BIN" "$NODE:$TARGET"
        echo "$CMD" | $SSH_CMD "$NODE" sudo bash
        { set +x; } 2>/dev/null
    done
}

function deploy {
    NODES="$1"
    for NODE in $NODES ; do
        CMD="chown root.root $TARGET && chmod +x $TARGET"
        CMD="$CMD && mv /tmp/$SRV_FILE $SRV_FILE_DST_PATH && chown root.root $SRV_FILE_DST_PATH"
        CMD="$CMD && systemctl daemon-reload && systemctl enable $SERVICE && systemctl start $SERVICE"

        set -x
        $SCP_CMD "$BIN" "$NODE:$TARGET"
        cat "$SRV_FILE" | sed --expression "s/{DURATION}/$RECORD_DURATION/" \
                              --expression "s/{SIZE}/$RECORD_SIZE/" \
                              --expression "s/{LOG_FILE}/${LOG//\//\\/}/" \
                              --expression "s/{RESULT}/${RESULT//\//\\/}/" \
                        | $SSH_CMD "$NODE" "cat > /tmp/$SRV_FILE"
        echo "$CMD" | $SSH_CMD "$NODE" sudo bash
        { set +x; } 2>/dev/null
    done
}

function show {
    NODES="$1"
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    NC='\033[0m' # No Color

    for NODE in $NODES ; do
        SRV_STAT=$($SSH_CMD $NODE sudo systemctl status $SERVICE | grep Active || true)
        LOG_FILE_LL=$($SSH_CMD $NODE ls -l $RESULT 2>&1 || true)

        if [[ "$LOG_FILE_LL" == *"No such file or directory"* ]] ; then
            LOG_SIZE="NO FILE"
        else
            LOG_SIZE=$(echo "$LOG_FILE_LL" | awk '{print $5}' | numfmt --to=iec-i --suffix=B)
        fi

        if [[ "$SRV_STAT" == *" inactive "* ]] ; then
            printf "%-20s : %b %s %b data_sz = %s\n" "$NODE" "$RED" "$SRV_STAT" "$NC" "$LOG_SIZE"
        else
            if [[ "$SRV_STAT" == *" failed "* ]] ; then
                printf "%-20s : %b %s %b data_sz = %s\n" "$NODE" "$RED" "$SRV_STAT" "$NC" "$LOG_SIZE"
            else
                printf "%-20s : %b %s %b data_sz = %s\n" "$NODE" "$GREEN" "$SRV_STAT" "$NC" "$LOG_SIZE"
            fi
        fi
    done
}

function stop {
    NODES="$1"
    for NODE in $NODES ; do
        set -x
        $SSH_CMD "$NODE" sudo systemctl stop "$SERVICE"
        { set +x; } 2>/dev/null
    done
}

function tails {
    NODES="$1"
    TSIZE="$2"
    for NODE in $NODES ; do
        echo $NODE
        $SSH_CMD "$NODE" tail --lines "$TSIZE" "$LOG"
        echo
    done
}

function start {
    NODES="$1"
    for NODE in $NODES ; do
        set -x
        $SSH_CMD "$NODE" sudo systemctl start "$SERVICE"
        { set +x; } 2>/dev/null
    done
}

function collect_one {
    NODE="$1"
    RBN=$(basename ${RESULT})
    set -x
    $SSH_CMD "$NODE" sudo "$COMPRESSOR" "$RESULT"
    $SCP_CMD "$NODE:${RESULT}.${EXT}" "${NODE}-${RBN}.${EXT}"
    $SSH_CMD "$NODE" sudo rm -f "${RESULT}.${EXT}"
    { set +x; } 2>/dev/null
}

function collect {
    for NODE in $1 ; do
        collect_one "$NODE"
    done
}

function collect_parallel {
    JOBS="$1"
    NODES="$2"
    NODES_COUNT=$(echo "$NODES" | wc --words)

    MAX_NODE_PER_JOB=$((NODES_COUNT / JOBS))
    if (( MAX_NODE_PER_JOB * JOBS != NODES_COUNT )); then
        MAX_NODE_PER_JOB=$((MAX_NODE_PER_JOB + 1))
    fi

    CNODES=""
    COUNT=0
    for NODE in $NODES ; do
        CNODES="$CNODES $NODE"
        COUNT=$((COUNT + 1))
        if (( COUNT == MAX_NODE_PER_JOB )) ; then
            collect "$CNODES" &
            CNODES=""
            COUNT=0
        fi
    done

    if [[ "$CNODES" != "" ]] ; then
        collect "$CNODES" &
    fi

    wait
}

while getopts "rcdsut:STlL" opt; do
    case "$opt" in
    c)  clean "$ALL_NODES"
        ;;
    u)  update_bin "$ALL_NODES"
        ;;
    l)  collect "$ALL_NODES"
        ;;
#    L)  collect_parallel $(echo $ALL_NODES | wc -w) "$ALL_NODES"
    L)  collect_parallel 2 "$ALL_NODES"
        ;;
    r)  clean "$ALL_NODES"
        deploy "$ALL_NODES"
        show "$ALL_NODES"
        ;;
    d)  deploy "$ALL_NODES"
        ;;
    s)  show "$ALL_NODES"
        ;;
    t)  tails "$ALL_NODES" "$OPTARG"
        ;;
    S)  start "$ALL_NODES"
        ;;
    T)  stop "$ALL_NODES"
        ;;
    esac
done
