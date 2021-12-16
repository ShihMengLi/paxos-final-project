# pkill -f "paxos_proposers.py"; pkill -f "paxos_acceptors.py"; pkill -f "test_run.py";
fuser -k 10203/tcp
fuser -k 10213/tcp
fuser -k 10223/tcp
fuser -k 10233/tcp
fuser -k 10243/tcp
fuser -k 10253/tcp
