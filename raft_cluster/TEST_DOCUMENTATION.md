# Raft Implementation - Test Cases Documentation

This document describes the 5 comprehensive test cases designed to validate the Raft consensus algorithm implementation in response to Q5 of the Project 3 description.

---

## Test Case 1: Normal Leader Election

### Objective
Verify that the cluster successfully elects a leader during normal startup conditions.

### Description
This test validates the fundamental leader election mechanism in Raft. When all nodes start simultaneously, they should elect exactly one leader through the voting process.

### Test Steps
1. Start all 5 nodes in the cluster simultaneously
2. Wait for the election timeout period (1.5-3 seconds)
3. Verify exactly one node becomes the leader
4. Confirm all other nodes recognize this leader as the current leader
5. Check that followers are receiving heartbeats from the leader

### Expected Results
- ✅ Exactly one node transitions to LEADER state
- ✅ All other nodes remain in FOLLOWER state
- ✅ Leader sends periodic AppendEntries (heartbeat) RPCs every 1 second
- ✅ Followers receive and acknowledge heartbeats
- ✅ No election timeout occurs while leader is alive

### Validation Criteria
- Log message: "Node X: Becoming LEADER for term Y"
- Follower logs show: "Node X runs RPC AppendEntries called by Node Y"
- Leader election completes within 5 seconds of startup

---

## Test Case 2: Log Replication with Client Operations

### Objective
Verify that log entries are correctly replicated across all nodes in the cluster.

### Description
This test validates the core log replication mechanism. When a client sends operations to the leader, those operations should be appended to the leader's log, replicated to followers, committed upon majority acknowledgment, and executed on all nodes.

### Test Steps
1. Identify the current leader in the cluster
2. Send multiple operations to the leader:
   - `name=Alice`
   - `age=30`
   - `city=NewYork`
   - `status=active`
3. Wait for the leader to replicate entries to followers
4. Verify operations are committed on the leader (majority ACKs received)
5. Check that all followers have replicated the log entries
6. Confirm operations are executed on all nodes' state machines

### Expected Results
- ✅ Leader appends operations to its log
- ✅ Leader sends AppendEntries RPCs with log entries on next heartbeat
- ✅ Followers append entries to their logs
- ✅ Followers send ACK (success=true) to leader
- ✅ Leader commits entries after receiving majority ACKs
- ✅ All nodes execute committed operations
- ✅ State machines are consistent across all nodes

### Validation Criteria
- Leader logs: "Appended operation to log at index X"
- Leader logs: "Committing entries up to index X"
- Leader logs: "Executing operation: X"
- Follower logs: "Appended N entries to log"
- All operations return success to client

---

## Test Case 3: Leader Failure and Re-election

### Objective
Verify that the cluster can detect leader failure and elect a new leader to maintain availability.

### Description
This test validates Raft's fault tolerance. When the current leader fails, the remaining nodes should detect the failure (via election timeout) and elect a new leader from among themselves.

### Test Steps
1. Identify the current leader (e.g., Node 2)
2. Stop the leader container: `docker stop raft_node2`
3. Wait for followers to timeout (election timeout: 1.5-3 seconds)
4. Observe the re-election process
5. Verify a new leader is elected from remaining nodes
6. Send a test operation to the new leader
7. Confirm cluster is fully functional with new leader
8. Restart the old leader: `docker start raft_node2`
9. Verify old leader rejoins as a follower

### Expected Results
- ✅ Followers detect missing heartbeats within election timeout
- ✅ Candidate increments term and starts election
- ✅ New leader is elected within 5 seconds
- ✅ New leader sends heartbeats to all nodes
- ✅ Cluster accepts client operations under new leader
- ✅ Old leader rejoins as follower when restarted
- ✅ Old leader adopts new term from current leader

### Validation Criteria
- Logs show: "Election timeout! Starting election..."
- New leader logs: "Becoming LEADER for term Y" (Y > previous term)
- Client operations succeed with new leader
- Old leader logs show becoming follower upon restart

---

## Test Case 4: Client Request Forwarding from Follower

### Objective
Verify that non-leader nodes correctly handle client requests by providing leader information for redirection.

### Description
In Raft, only the leader can process client requests. When a client sends a request to a follower, the follower should inform the client of the current leader's identity so the client can retry the request.

### Test Steps
1. Identify the current leader (e.g., Node 2)
2. Identify a follower node (e.g., Node 0)
3. Send a client operation to the follower: `forwarding_test=value`
4. Verify follower does NOT execute the operation
5. Confirm follower returns leader_id in the response
6. Client automatically retries the operation with the correct leader
7. Verify operation succeeds on the leader

### Expected Results
- ✅ Follower receives client request
- ✅ Follower logs: "Received client request"
- ✅ Follower logs: "Forwarding request to leader (Node X)"
- ✅ Follower response: success=false, leader_id=X
- ✅ Client retries with correct leader
- ✅ Operation succeeds on leader
- ✅ Log is replicated to all nodes including original follower

### Validation Criteria
- Follower response contains correct leader_id
- Client successfully completes operation after redirection
- Operation appears in logs on all nodes

---

## Test Case 5: Network Partition Recovery (Minority Node Rejoining)

### Objective
Verify that a node can successfully rejoin the cluster after being temporarily disconnected and catch up on missed log entries.

### Description
This test simulates a temporary network partition where one node is isolated. The remaining nodes (still forming a majority) continue to operate and commit new entries. When the isolated node rejoins, it should catch up with the current state.

### Test Steps
1. Identify current leader and select a follower to isolate (e.g., Node 3)
2. Stop the follower: `docker stop raft_node3`
3. While node is down, send operations to the leader:
   - `op1=value1`
   - `op2=value2`
   - `op3=value3`
4. Verify cluster commits these operations (4 nodes = majority)
5. Restart the isolated node: `docker start raft_node3`
6. Wait for node to reconnect
7. Verify node receives AppendEntries with missing log entries
8. Confirm node's log catches up to match leader
9. Send one final operation to verify full recovery

### Expected Results
- ✅ Cluster continues operating with 4 nodes (majority intact)
- ✅ Operations are committed while node is down
- ✅ Rejoining node connects to current leader
- ✅ Leader sends AppendEntries with all missing entries
- ✅ Node appends missing entries to its log
- ✅ Node's commit index advances to match leader
- ✅ Node executes all missed operations
- ✅ Cluster fully operational with all 5 nodes

### Validation Criteria
- Operations succeed while node is down (4 node majority)
- Rejoining node logs: "runs RPC AppendEntries called by Node X"
- Rejoining node logs: "Appended N entries to log"
- Final test operation succeeds
- All 5 nodes have identical logs

---

## Running the Tests

### Prerequisites
- Docker and Docker Compose installed
- Python 3.9+ with grpcio installed
- Raft cluster built and ready

### Execution

Run all tests:
```bash
cd /Path/to/raft_cluster
python3 test_raft.py
```

Run individual tests (modify the script to comment out unwanted tests).

### Capturing Screenshots

For documentation purposes, capture screenshots at key moments:

1. **Test 1**: Screenshot showing "Becoming LEADER" log entry
2. **Test 2**: Screenshot showing log replication and commit messages
3. **Test 3**: Screenshots of leader failure, election, and new leader
4. **Test 4**: Screenshot showing request forwarding response
5. **Test 5**: Screenshots of node rejoining and catching up

Use:
```bash
# Capture container logs
docker logs raft_node0 > test1_leader_election.log

# Watch logs in real-time
docker-compose logs -f | tee test_output.log
```

---

## Expected Test Output Summary

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║            RAFT CONSENSUS ALGORITHM - COMPREHENSIVE TEST SUITE               ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

[SETUP] Ensuring clean cluster state...

================================================================================
  TEST: Test Case 1: Normal Leader Election
================================================================================

[Step 1] Starting all 5 nodes in the cluster
...
✅ SUCCESS: Node 2 elected as leader

[Step 2] Verified: All followers receiving heartbeats

TEST PASSED
--------------------------------------------------------------------------------

================================================================================
  TEST: Test Case 2: Log Replication with Client Operations
================================================================================

[Step 1] Finding current leader
Leader is Node 2

[Step 2] Sending operations to leader
Sending: name=Alice
  ✅ SUCCESS: Operation committed and executed on leader
...
✅ TEST PASSED: 4/4 operations committed and replicated

--------------------------------------------------------------------------------

[... Tests 3, 4, 5 ...]

╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                              TEST SUMMARY                                    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

✅ Test 1: Normal Leader Election: PASSED
✅ Test 2: Log Replication: PASSED
✅ Test 3: Leader Failure & Re-election: PASSED
✅ Test 4: Request Forwarding: PASSED
✅ Test 5: Node Rejoin: PASSED

Overall: 5/5 tests passed
```

---

## Test Coverage Analysis

These 5 test cases comprehensively validate:

| Raft Feature | Test Case(s) |
|--------------|--------------|
| Leader Election | 1, 3 |
| Log Replication | 2, 5 |
| Heartbeats | 1, 2, 3, 5 |
| Fault Tolerance | 3, 5 |
| Request Forwarding | 4 |
| Majority Consensus | 2, 3, 5 |
| State Machine Execution | 2, 5 |
| Term Management | 1, 3 |
| Log Consistency | 2, 5 |
| Cluster Recovery | 3, 5 |

**Total Coverage**: All core Raft mechanisms are tested across the 5 test cases.
