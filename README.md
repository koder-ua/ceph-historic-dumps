Tool to collect ceph slow ops execution traces from cluster
-----------------------------------------------------------

How to collect data:

    * You need user with passwordless ssh to all required osd nodes and passwordless sudo on all of them
    * create inventory file - file, with list of all ceph osd nodes one per line by name or by ip,
      on which sshd listening
    * Run 'git clone https://github.com/koder-ua/ceph-historic-dumps.git' on any node,
      which has an access to all osd nodes
    * cd ceph-historic-dumps
    * git checkout stable
    * 'bash ./deploy.sh -d -P INVENTORY_FILE' to deploy
    * 'bash ./deploy.sh -S -P INVENTORY_FILE' to start services
    * 'bash ./deploy.sh -s -P INVENTORY_FILE' to check current status - all nodes should run daemon
      and log file should grows at about 1-10KiBps per minute speed
    * 'bash ./deploy.sh -l -P INVENTORY_FILE' to collect records file from all nodes
    * 'bash ./deploy.sh -c -P INVENTORY_FILE' to stop logger and clean all nodes (you need to collect logs first)


How to create report:

    * Checkout stable label, as described above
    * python3.X -m venv collect_env
    * source collect_env/bin/activate
    * cd ceph-historic-dumps
    * pip install -r requirements.txt
    * python parse_historic_ops.py tohdfs HDF_FILE ALL_RECORD_FILES
    * python parse_historic_ops.py report --all HDF_FILE