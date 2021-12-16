# pkill -f "paxos_proposers.py"; pkill -f "paxos_acceptors.py"; pkill -f "test_run.py";
fuser -k 10200/tcp
fuser -k 10210/tcp
fuser -k 10220/tcp
fuser -k 10230/tcp
fuser -k 10240/tcp
fuser -k 10250/tcp
