# pkill -f "paxos_proposers.py"; pkill -f "paxos_acceptors.py"; pkill -f "test_run.py";
fuser -k 10202/tcp
fuser -k 10212/tcp
fuser -k 10222/tcp
fuser -k 10232/tcp
fuser -k 10242/tcp
fuser -k 10252/tcp
