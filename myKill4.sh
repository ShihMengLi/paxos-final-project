# pkill -f "paxos_proposers.py"; pkill -f "paxos_acceptors.py"; pkill -f "test_run.py";
fuser -k 10204/tcp
fuser -k 10214/tcp
fuser -k 10224/tcp
fuser -k 10234/tcp
fuser -k 10244/tcp
fuser -k 10254/tcp
