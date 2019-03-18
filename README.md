Tool to collect ceph slow ops execution traces from cluster
-----------------------------------------------------------

How to collect data:

    * You need user with passwordless ssh to all required osd nodes and passwordless sudo on all of them
    * create inventory file - file, with list of all ceph osd nodes one per line by name or by ip,
      on which sshd listening
    * If you can run 'git clone':
        - Run 'git clone https://github.com/koder-ua/ceph-historic-dumps.git' on any node,
      which has an access to all osd nodes
        - cd ceph-historic-dumps
        - git checkout stable
    * If you can't run 'git clone' (beware this one is downloading current master branch)
        - curl -L https://api.github.com/repos/koder-ua/ceph-historic-dumps/tarball -o ceph-historic-dumps.tar.gz
        - mkdir ceph-historic-dumps && cd ceph-historic-dumps
        - tar --strip-components=1 -xvzf ../ceph-historic-dumps.tar.gz
    * './ctl -d -P INVENTORY_FILE' to deploy
    * './ctl -S -P INVENTORY_FILE' to start services
    * './ctl -s -P INVENTORY_FILE' to check current status - all nodes should run daemon
      and log file should grows at about 1-10KiBps per minute speed
    * './ctl -l -P INVENTORY_FILE' to collect records file from all nodes
    * './ctl -c -P INVENTORY_FILE' to stop logger and clean all nodes (you need to collect logs first)


How to create report:

    * Checkout stable label, as described above
    * python3.6 -m venv collect_env
    * source collect_env/bin/activate
    * cd ceph-historic-dumps
    * pip install -r requirements.txt
    * python -m collect_historic_ops.hdfutil tohdfs HDF_FILE ALL_RECORD_FILES
    * python -m collect_historic_ops.report report --all HDF_FILE