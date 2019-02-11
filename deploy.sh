#!/usr/bin/env bash
set -e
set -o pipefail

#COMPRESSOR="gzip -f -k"
#EXT="gz"

COMPRESSOR="lzma -9 -z -k -f"
EXT="lzma"

BIN=ceph_ho_dumper.py
NODES="ceph01 ceph02 ceph03"
TARGET="/tmp/$BIN"
LOG=/tmp/ceph_ho_dumper.log
RESULT=/tmp/historic_ops_dump.bin

SRV_FILE=mira-ceph-ho-dumper.service
SERVICE="$SRV_FILE"
SRV_FILE_DST_PATH="/lib/systemd/system/$SRV_FILE"

DEFAULT_COUNT=20
DEFAULT_DURATION=600

DURATION=60
SIZE=100


function clean {
    set -x
    set +e
    for node in $NODES ; do
        CMD="systemctl stop $SERVICE ; systemctl disable $SERVICE"
        CMD="$CMD ; $TARGET set --duration=$DEFAULT_DURATION --count=$DEFAULT_COUNT >/dev/null 2>&1 "
        CMD="$CMD ; rm -f $SRV_FILE_DST_PATH $TARGET $LOG $RESULT"
        echo "$CMD" | ssh "$node" sudo bash
    done
    set -e
}


function update_bin {
    set -x
    for node in $NODES ; do
        ssh "$node" sudo systemctl stop "$SERVICE"
        scp "$BIN" "$node:$TARGET"
        CMD="chown root.root $TARGET && chmod +x $TARGET && sudo systemctl start $SERVICE"
        echo "$CMD" | ssh "$node" sudo bash
    done
}


function deploy {
    set -x
    for node in $NODES ; do
        scp "$BIN" "$node:$TARGET"
        cat "$SRV_FILE" | sed -e "s/{DURATION}/$DURATION/" -e "s/{SIZE}/$SIZE/" \
            -e "s/{LOG_FILE}/${LOG//\//\\/}/" -e "s/{RESULT}/${RESULT//\//\\/}/" | ssh "$node" "cat > /tmp/$SRV_FILE"

        CMD="chown root.root $TARGET && chmod +x $TARGET"
        CMD="$CMD && mv /tmp/$SRV_FILE $SRV_FILE_DST_PATH && chown root.root $SRV_FILE_DST_PATH"
        CMD="$CMD && systemctl daemon-reload && systemctl enable $SERVICE && systemctl start $SERVICE"
        echo "$CMD" | ssh "$node" sudo bash
    done
}

function show {
    set +x
    set +e

    RED='\033[0;31m'
    GREEN='\033[0;32m'
    NC='\033[0m' # No Color

    for node in $NODES ; do
        SRV_STAT=$(ssh $node sudo systemctl status $SERVICE | grep Active)
        LOG_FILE_LL=$(ssh $node ls -l $RESULT 2>&1)

        if [[ "$LOG_FILE_LL" == *"No such file or directory"* ]] ; then
            LOG_SIZE="NO FILE"
        else
            LOG_SIZE=$(echo "$LOG_FILE_LL" | awk '{print $5}')
        fi

        if [[ "$SRV_STAT" == *" inactive "* ]] ; then
            printf "%-20s : %b %s %b LOG_FILE_SZ=%s\n" "$node" "$RED" "$SRV_STAT" "$NC" "$LOG_SIZE"
        else
            if [[ "$SRV_STAT" == *" failed "* ]] ; then
                printf "%-20s : %b %s %b LOG_FILE_SZ=%s\n" "$node" "$RED" "$SRV_STAT" "$NC" "$LOG_SIZE"
            else
                printf "%-20s : %b %s %b LOG_FILE_SZ=%s\n" "$node" "$GREEN" "$SRV_STAT" "$NC" "$LOG_SIZE"
            fi
        fi
    done
}

function stop {
    set +x
    for node in $NODES ; do
        ssh "$node" sudo systemctl stop "$SERVICE"
    done
}

function tails {
    set +x
    for node in $NODES ; do
        echo $node
        ssh "$node" tail -n "$1" "$LOG"
        echo
    done
}

function start {
    set -x
    for node in $NODES ; do
        ssh "$node" sudo systemctl start "$SERVICE"
    done
}

function collect_one {
    node="$1"
    ssh "$node" sudo "$COMPRESSOR" "$RESULT"
    scp "$node:${RESULT}.${EXT}" "${node}-$(basename ${RESULT}).${EXT}"
    ssh "$node" sudo rm -f "${RESULT}.${EXT}"
}

function collect {
    set -x
    for node in $NODES ; do
        collect_one "$node"
    done
}

function collect_parallel {
    set -x
    for node in $NODES ; do
        collect_one "$node" &
    done
    wait
}

set +x
while getopts "rcdsut:STlL" opt; do
    case "$opt" in
    c)  clean
        ;;
    u)  update_bin
        ;;
    l)  collect
        ;;
    L)  collect_parallel
        ;;
    r)  clean
        deploy
        show
        ;;
    d)  deploy
        ;;
    s)  show
        ;;
    t)  tails $OPTARG
        ;;
    S)  start
        ;;
    T)  stop
        ;;
    esac
done

