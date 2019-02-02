#!/usr/bin/env bash
set -xe

if [[ $(hostname) == "ceph20" ]] ; then
    OSDS=$(ceph osd tree | grep -A14 $(hostname) | grep sata2 | awk '{print $1}')
else
    OSDS=$(ceph osd tree | grep -A16 $(hostname) | grep sata2 | awk '{print $1}')
fi
#OSDS=$(ceph osd tree | grep -A1 $(hostname) | grep hdd | awk '{print $1}')

function set {
    DUR=$1
    SZ=$2
    for OSD in $OSDS ; do
        ceph daemon osd.$OSD config set osd_op_history_duration $DUR
        ceph daemon osd.$OSD config set osd_op_history_size $SZ
    done
}

function dump {
    DURATION=$1
    OUTPUT=$2
    while true ; do
        for OSD in $OSDS ; do
            ceph daemon osd.$OSD dump_historic_ops >> $OUTPUT
        done
        sleep $DURATION
    done
}

if [[ $1 == "dump" ]] ; then
    OUTPUT=$2
    DURATION=$3
    COUNT=$4
    LOG_FILE=$5

    set $DURATION $COUNT 2>&1 >> $LOG_FILE
    dump $DURATION $OUTPUT 2>&1 >> $LOG_FILE
elif [[ $1 == "set" ]] ; then
    set $2 $3
else
    echo "Unknown cmd $1"
    exit 1
fi
