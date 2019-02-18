Tool to collect ceph slow ops execution traces from cluster
-----------------------------------------------------------

How to use:

    * You need user with passwordless ssh to all required osd nodes and with passwordless sudo on them
    * Run 'git clone https://github.com/koder-ua/ceph-historic-dumps.git' on any node,
      which has an access to all osd nodes
    * edit deploy.sh - replace line
        readonly ALL_NODES="ceph01 ceph02 ceph03"
      with list of all required osd node names
    * 'bash ./deploy.sh -d' to deploy and start monitoring
    * 'bash ./deploy.sh -s' to check current status - all nodes should run daemon
      and log file should grows at about 1-10KiBps per minute speed
    * 'bash ./deploy.sh -L' to collect logs from all nodes
    * 'bash ./deploy.sh -c' to stop logger and clean all nodes (you need to collect logs first)

