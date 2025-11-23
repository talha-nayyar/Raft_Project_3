#!/bin/bash

echo "=========================================="
echo "Raft Cluster Test Script"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Step 1: Building and starting Raft cluster (5 nodes)${NC}"
docker-compose up --build -d

echo ""
echo -e "${YELLOW}Step 2: Waiting for cluster to start and elect leader (10 seconds)${NC}"
sleep 10

echo ""
echo -e "${GREEN}Step 3: Checking node status${NC}"
echo "Node 0:"
docker logs raft_node0 2>&1 | grep -E "(initialized|LEADER|FOLLOWER|CANDIDATE)" | tail -3

echo ""
echo "Node 1:"
docker logs raft_node1 2>&1 | grep -E "(initialized|LEADER|FOLLOWER|CANDIDATE)" | tail -3

echo ""
echo "Node 2:"
docker logs raft_node2 2>&1 | grep -E "(initialized|LEADER|FOLLOWER|CANDIDATE)" | tail -3

echo ""
echo "Node 3:"
docker logs raft_node3 2>&1 | grep -E "(initialized|LEADER|FOLLOWER|CANDIDATE)" | tail -3

echo ""
echo "Node 4:"
docker logs raft_node4 2>&1 | grep -E "(initialized|LEADER|FOLLOWER|CANDIDATE)" | tail -3

echo ""
echo -e "${GREEN}Step 4: Checking for leader${NC}"
LEADER=$(docker-compose logs 2>&1 | grep "Becoming LEADER" | tail -1)
if [ -z "$LEADER" ]; then
    echo "No leader elected yet. Waiting more..."
    sleep 5
    LEADER=$(docker-compose logs 2>&1 | grep "Becoming LEADER" | tail -1)
fi
echo "$LEADER"

echo ""
echo -e "${GREEN}Step 5: Cluster is ready!${NC}"
echo "You can now:"
echo "  - View logs: docker-compose logs -f"
echo "  - View specific node: docker logs -f raft_node0"
echo "  - Run client: docker exec -it raft_client python3 raft_client.py"
echo "  - Stop cluster: docker-compose down"

echo ""
echo -e "${YELLOW}To interact with the cluster, run:${NC}"
echo "  docker exec -it raft_client python3 raft_client.py"
