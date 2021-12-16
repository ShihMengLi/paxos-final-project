# pkill -f "paxos_proposers.py"; pkill -f "paxos_acceptors.py"; pkill -f "test_run.py";
fuser -k 10201/tcp
fuser -k 10211/tcp
fuser -k 10221/tcp
fuser -k 10231/tcp
fuser -k 10241/tcp
fuser -k 10251/tcp
