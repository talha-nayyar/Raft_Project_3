# Raft Consensus Algorithm Implementation

An implementation of the Raft consensus algorithm featuring leader election, log replication, and fault tolerance. This implementation is on the Group 21 Distributed E-Commerce System, and runs in a containerized 5-node cluster and provides a complete distributed key-value store.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Usage Guide](#usage-guide)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Implementation Details](#implementation-details)

---

## Overview

This Raft implementation provides:
- **Leader Election**: Automatic leader election with randomized timeouts
- **Log Replication**: Consistent log replication across all nodes
- **Fault Tolerance**: Survives up to 2 node failures in a 5-node cluster
- **Request Forwarding**: Automatic forwarding of client requests to the leader
- **State Machine**: Simple key-value store as the replicated state machine

---

## Features

### Leader Election (Q3)

- **Three Node States**: Follower, Candidate, and Leader
- **Timeout Configuration**:
  - Heartbeat interval: 1 second
  - Election timeout: Randomized between 1.5-3 seconds per node
- **Election Process**:
  - Nodes start as followers
  - Transition to candidate on election timeout
  - Request votes from peers with incremented term
  - Become leader upon receiving majority votes
  - Send periodic heartbeats to maintain leadership

### Log Replication (Q4)

- **Append-Only Logs**: Each node maintains an ordered log of operations
- **Replication Flow**:
  1. Leader receives client request
  2. Leader appends to local log
  3. Leader replicates to followers via AppendEntries RPC
  4. Followers acknowledge receipt
  5. Leader commits after majority acknowledgment
  6. Committed entries are executed on state machine
- **Request Forwarding**: Followers redirect clients to the current leader

### RPC Logging

All RPC calls are logged for debugging and verification:
- **Client-side**: `Node X sends RPC <name> to Node Y`
- **Server-side**: `Node X runs RPC <name> called by Node Y`

---

## Quick Start

### Prerequisites

- Docker (version 20.10 or higher)
- Docker Compose (version 1.29 or higher)

Verify installation:
```bash
docker --version
docker-compose --version
```

### Installation

1. **Clone or navigate to the project directory**:
```bash
cd /path/to/raft_cluster
```

2. **Build and start the cluster**:
```bash
docker-compose up --build -d
```

3. **Verify all nodes are running**:
```bash
docker ps | grep raft_node
```

Expected output:
```
raft_node0    Up
raft_node1    Up
raft_node2    Up
raft_node3    Up
raft_node4    Up
```

4. **Watch the leader election** (wait ~5-10 seconds):
```bash
docker-compose logs | grep "Becoming LEADER"
```

Expected output:
```
raft_node2  | Node 2: Becoming LEADER for term 1
```

### First Operations

Send operations to the cluster:
```bash
docker exec -it raft_client python3 raft_client.py
```

Try these commands:
```
Enter operation: name=Alice
SUCCESS: Operation committed and executed on leader

Enter operation: age=30
SUCCESS: Operation committed and executed on leader

Enter operation: city=NewYork
SUCCESS: Operation committed and executed on leader
```

Press `Ctrl+C` to exit the client.

---

## Architecture

### Cluster Configuration

| Node | Container Name | Port | Role |
|------|---------------|------|------|
| Node 0 | raft_node0 | 50060 | Follower/Candidate/Leader |
| Node 1 | raft_node1 | 50061 | Follower/Candidate/Leader |
| Node 2 | raft_node2 | 50062 | Follower/Candidate/Leader |
| Node 3 | raft_node3 | 50063 | Follower/Candidate/Leader |
| Node 4 | raft_node4 | 50064 | Follower/Candidate/Leader |
| Client | raft_client | N/A | Client interface |

### File Structure

```
raft_cluster/
├── raft.proto              # gRPC service definitions
├── raft_node.py            # Raft node implementation
├── raft_client.py          # Client for sending operations
├── Dockerfile              # Container image definition
├── docker-compose.yml      # Cluster orchestration
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── test_raft.py           # Automated test suite
└── [TEST_*.md]            # Test documentation
```

### gRPC Services

**RequestVote RPC** - Used for leader election:
```protobuf
message VoteRequest {
  int32 term = 1;
  int32 candidate_id = 2;
}
```

**AppendEntries RPC** - Used for heartbeats and log replication:
```protobuf
message AppendEntriesRequest {
  int32 term = 1;
  int32 leader_id = 2;
  repeated LogEntry entries = 3;
  int32 leader_commit = 4;
}
```

**ExecuteOperation RPC** - Client request handler:
```protobuf
message OperationRequest {
  string operation = 1;
}
```

---

## Usage Guide

### Starting the Cluster

**Standard start** (with logs):
```bash
docker-compose up --build
```

**Background mode** (detached):
```bash
docker-compose up --build -d
```

**Force rebuild** (after code changes):
```bash
docker-compose build --no-cache
docker-compose up -d
```

### Monitoring the Cluster

**View all logs**:
```bash
docker-compose logs -f
```

**View specific node logs**:
```bash
docker logs -f raft_node0
docker logs -f raft_node2
```

**Check cluster status**:
```bash
docker ps | grep raft
```

**Find current leader**:
```bash
docker-compose logs | grep "Becoming LEADER" | tail -1
```

**Monitor heartbeats**:
```bash
docker logs raft_node0 | grep "AppendEntries" | head -20
```

**Check log replication**:
```bash
docker logs raft_node2 | grep -E "(Committing|Executing)" | tail -10
```

### Sending Client Requests

**Interactive mode**:
```bash
docker exec -it raft_client python3 raft_client.py
```

**Single operation**:
```bash
echo "status=active" | docker exec -i raft_client python3 raft_client.py
```

**Operation format**: `key=value`
- Examples: `x=10`, `name=Alice`, `count=42`, `status=active`

### Stopping the Cluster

**Graceful shutdown**:
```bash
docker-compose down
```

**Force stop and cleanup**:
```bash
docker-compose down
docker system prune -f
```

---

## Testing

### Automated Test Suite

Run all 5 comprehensive test cases:

```bash
# One-time setup
bash setup_tests.sh

# Run tests
python3 test_raft.py
```

**Expected output**:
```
╔══════════════════════════════════════════════════════════════╗
║        RAFT CONSENSUS ALGORITHM - COMPREHENSIVE TEST SUITE   ║
╚══════════════════════════════════════════════════════════════╝

P Test 1: Normal Leader Election: PASSED
P Test 2: Log Replication: PASSED
P Test 3: Leader Failure & Re-election: PASSED
P Test 4: Request Forwarding: PASSED
P Test 5: Node Rejoin: PASSED

Overall: 5/5 tests passed
```

### Test Cases

1. **Normal Leader Election**: Verifies initial leader election and heartbeat mechanism
2. **Log Replication**: Tests client operations and log consistency across nodes
3. **Leader Failure & Re-election**: Validates fault tolerance and automatic re-election
4. **Request Forwarding**: Tests non-leader request redirection
5. **Node Rejoin**: Validates recovery after network partition

For detailed test documentation, see:
- [RUNNING_TESTS.md](RUNNING_TESTS.md) - How to run tests
- [TEST_DOCUMENTATION.md](TEST_DOCUMENTATION.md) - Test specifications
- [MANUAL_TEST_GUIDE.md](MANUAL_TEST_GUIDE.md) - Manual testing steps

### Manual Testing Scenarios

#### Test 1: Normal Operation
```bash
# Start cluster
docker-compose up -d

# Wait for leader election
sleep 10

# Check leader
docker-compose logs | grep "Becoming LEADER"

# Send operations
docker exec -it raft_client python3 raft_client.py
# Enter: x=10, y=20, z=30

# Verify replication on all nodes
for i in {0..4}; do
  echo "Node $i:"
  docker logs raft_node$i | grep "Executing operation" | tail -3
done
```

#### Test 2: Leader Failure
```bash
# Find current leader
LEADER=$(docker-compose logs | grep "Becoming LEADER" | tail -1 | grep -oP 'Node \K[0-9]')
echo "Current leader: Node $LEADER"

# Stop leader
docker stop raft_node$LEADER

# Wait for re-election
sleep 10

# Verify new leader
docker-compose logs | grep "Becoming LEADER" | tail -1

# Test operations still work
docker exec -it raft_client python3 raft_client.py
# Enter: test=failure_recovery

# Restart old leader
docker start raft_node$LEADER

# Verify it rejoins as follower
sleep 5
docker logs raft_node$LEADER | grep "AppendEntries" | tail -5
```

#### Test 3: Network Partition
```bash
# Stop 2 nodes (minority)
docker stop raft_node0 raft_node1

# Verify cluster still works (3 nodes = majority)
docker exec -it raft_client python3 raft_client.py
# Enter: partition=test

# Restart nodes
docker start raft_node0 raft_node1

# Verify they catch up
sleep 10
docker logs raft_node0 | grep "Executing operation" | tail -5
```

---

## Troubleshooting

### No Leader Elected

**Symptoms**: No node becomes leader after 10+ seconds

**Solutions**:
1. **Check all nodes are running**:
   ```bash
   docker ps | grep raft_node
   ```

2. **Ensure majority are healthy**:
   ```bash
   docker-compose ps
   ```
   At least 3 of 5 nodes must be "Up"

3. **Check for network issues**:
   ```bash
   docker network inspect raft_cluster_raft_network
   ```

4. **Restart cluster**:
   ```bash
   docker-compose down
   docker-compose up --build -d
   sleep 15
   docker-compose logs | grep LEADER
   ```

### Client Operations Timeout

**Symptoms**: Client requests fail with timeout errors

**Solutions**:
1. **Verify leader exists**:
   ```bash
   docker-compose logs | grep "Becoming LEADER"
   ```

2. **Check majority nodes running**:
   ```bash
   docker ps | grep raft_node | wc -l
   ```
   Should show at least 3 nodes

3. **Verify client network**:
   ```bash
   docker exec raft_client ping -c 2 raft_node0
   ```

4. **Check leader logs**:
   ```bash
   # Find leader node ID from logs
   docker logs raft_node<ID> | grep -i error
   ```

### Protobuf Module Not Found

**Symptoms**: `ModuleNotFoundError: No module named 'raft_pb2'`

**Solutions**:
1. **Run setup script**:
   ```bash
   bash setup_tests.sh
   ```

2. **Or manually generate**:
   ```bash
   python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
   ```

### Logs Not Showing

**Symptoms**: Docker logs appear empty or delayed

**Solution**: Already fixed with `PYTHONUNBUFFERED=1` in the Docker configuration. If issues persist:
```bash
docker-compose build --no-cache
docker-compose up -d
```

### Port Conflicts

**Symptoms**: `bind: address already in use`

**Solutions**:
1. **Find and kill process using port**:
   ```bash
   lsof -i :50060
   kill -9 <PID>
   ```

2. **Or stop all Raft containers**:
   ```bash
   docker stop $(docker ps -aq --filter "name=raft_node")
   ```

### Containers Keep Restarting

**Symptoms**: `docker ps` shows containers constantly restarting

**Solutions**:
1. **Check container logs**:
   ```bash
   docker logs raft_node0
   ```

2. **Verify image built correctly**:
   ```bash
   docker-compose build --no-cache
   ```

3. **Check system resources**:
   ```bash
   docker system df
   ```

---

## Implementation Details

### State Machine

The implementation uses a simple key-value store:
```python
state = {}  # In-memory dictionary
```

**Operation format**: `key=value`

**Examples**:
- `name=Alice` → `state['name'] = 'Alice'`
- `age=30` → `state['age'] = '30'`
- `count=42` → `state['count'] = '42'`

### Log Entry Structure

Each log entry contains:
```python
{
    'term': int,        # Term when entry was created
    'index': int,       # Position in log (1-indexed)
    'operation': str    # Operation string (key=value)
}
```

### Node States

**FOLLOWER**:
- Initial state for all nodes
- Receives heartbeats and log entries from leader
- Votes in elections
- Forwards client requests to leader

**CANDIDATE**:
- Transitional state during elections
- Requests votes from peers
- Becomes leader if receives majority votes
- Returns to follower if election timeout or higher term discovered

**LEADER**:
- Accepts client requests
- Replicates log entries to followers
- Sends periodic heartbeats (empty AppendEntries)
- Steps down if higher term discovered

### Raft Safety Properties

This implementation ensures:

1. **Election Safety**: At most one leader per term
2. **Leader Append-Only**: Leaders never overwrite or delete log entries
3. **Log Matching**: If two logs contain entry with same index and term, logs are identical up to that point
4. **Leader Completeness**: If a log entry is committed in a term, it will be present in leaders of all higher terms
5. **State Machine Safety**: If a node has applied a log entry at a given index, no other node will apply a different entry for that index

### Performance Characteristics

- **Leader Election Time**: 1.5-3 seconds (randomized election timeout)
- **Heartbeat Interval**: 1 second
- **Operation Latency**: 1-2 seconds (one heartbeat round-trip)
- **Fault Tolerance**: Tolerates up to 2 simultaneous node failures (5-node cluster)
- **Throughput**: Limited by heartbeat interval and network latency

### Concurrency and Thread Safety

- **Lock-based synchronization**: `threading.Lock()` protects shared state
- **Background threads**:
  - Election timer thread (monitors heartbeat timeout)
  - Heartbeat thread (leader sends periodic AppendEntries)
- **Deadlock prevention**: Election logic runs outside lock context

---

## Additional Resources

- **Quick Start Guide**: [QUICK_START.md](QUICK_START.md)
- **Test Documentation**: [TEST_DOCUMENTATION.md](TEST_DOCUMENTATION.md)
- **Test Results**: [TEST_SUMMARY.md](TEST_SUMMARY.md)
- **Manual Testing**: [MANUAL_TEST_GUIDE.md](MANUAL_TEST_GUIDE.md)
- **Test Commands**: [QUICK_TEST_COMMANDS.md](QUICK_TEST_COMMANDS.md)
- **Running Tests**: [RUNNING_TESTS.md](RUNNING_TESTS.md)

---

## Project Information

**Implementation**: Raft Consensus Algorithm
**Language**: Python 3.9
**Framework**: gRPC
**Containerization**: Docker + Docker Compose
**Cluster Size**: 5 nodes (configurable)
**Test Coverage**: 5 comprehensive test cases

---

## External Refrences Used
- 1: https://www.geeksforgeeks.org/system-design/raft-consensus-algorithm/
- 2: Project 3 Descriptions
- 3: Docker Documentation
- 4: Claude Opus to generate biolerplate code for implementations and test suite environment (only) to help in ease of deploying the tests.
- 5: Google Gemini to develop documentation and debugging.

## License

This implementation is part of the Distributed Systems Project 3 - Raft Consensus Algorithm assignment for Group 22

---
