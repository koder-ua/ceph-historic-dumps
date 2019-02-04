#!/usr/bin/env bash
set -e
set -o pipefail

#COMPRESSOR="gzip -f -k"
#EXT="gz"

COMPRESSOR="lzma -9 -z -k -f"
EXT="lzma"

BIN=ceph-ho-dumper.py
NODES="ceph01 ceph02 ceph03"
TARGET="/tmp/$BIN"
LOG=/tmp/ceph_ho_dumper.log
RESULT=/tmp/historic_ops_dump.log

SRV_FILE=mira-ceph-ho-dumper.service
SERVICE="$SRV_FILE"
SRV_FILE_DST_PATH="/lib/systemd/system/$SRV_FILE"


function clean {
    set -x
    set +e
    for node in $NODES ; do
        CMD="systemctl stop $SERVICE ; systemctl disable $SERVICE"
        CMD="$CMD ; $TARGET set 600 20 >/dev/null 2>&1 "
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
        scp "$SRV_FILE" "$node:/tmp"

        CMD="sudo chown root.root $TARGET && sudo chmod +x $TARGET"
        CMD="$CMD && mv /tmp/$SRV_FILE $SRV_FILE_DST_PATH"
        CMD="$CMD && chown root.root $SRV_FILE_DST_PATH"
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
        if [[ $SRV_STAT == *" inactive "* ]] ; then
            printf "%-20s : %b %s %b\n" "$node" "$RED" "$SRV_STAT" "$NC"
        else
            if [[ $SRV_STAT == *" failed "* ]] ; then
                printf "%-20s : %b %s %b\n" "$node" "$RED" "$SRV_STAT" "$NC"
            else
                printf "%-20s : %b %s %b\n" "$node" "$GREEN" "$SRV_STAT" "$NC"
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

