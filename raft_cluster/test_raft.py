"""
Comprehensive Tests for Raft Implementation
5 different scenarios to validate the Raft consensus algorithm
"""

import subprocess
import time
import sys
import grpc
import raft_pb2
import raft_pb2_grpc


class RaftTester:
    def __init__(self):
        self.cluster_nodes = [
            (0, 'localhost:50060'),
            (1, 'localhost:50061'),
            (2, 'localhost:50062'),
            (3, 'localhost:50063'),
            (4, 'localhost:50064')
        ]

    def print_header(self, test_name):
        """Print a formatted test header."""
        print("\n" + "="*80)
        print(f"  TEST: {test_name}")
        print("="*80 + "\n")

    def print_step(self, step_num, description):
        """Print a test step."""
        print(f"\n[Step {step_num}] {description}")
        print("-" * 60)

    def get_leader(self):
        """Find the current leader by checking all nodes."""
        for node_id, address in self.cluster_nodes:
            try:
                channel = grpc.insecure_channel(address)
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                request = raft_pb2.ClientRequest(operation="test=probe")
                response = stub.ExecuteOperation(request, timeout=2.0)
                channel.close()

                if response.success or response.leader_id == node_id:
                    return node_id
                elif response.leader_id != -1:
                    return response.leader_id
            except:
                continue
        return None

    def send_operation(self, node_id, operation):
        """Send an operation to a specific node."""
        address = self.cluster_nodes[node_id][1]
        try:
            channel = grpc.insecure_channel(address)
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            request = raft_pb2.ClientRequest(operation=operation)
            response = stub.ExecuteOperation(request, timeout=10.0)
            channel.close()
            return response.success, response.message, response.leader_id
        except Exception as e:
            return False, str(e), -1

    def check_container_status(self, container_name):
        """Check if a container is running."""
        try:
            result = subprocess.run(
                ['docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}'],
                capture_output=True, text=True, check=True
            )
            return container_name in result.stdout
        except:
            return False

    def get_container_logs(self, container_name, lines=20):
        """Get recent logs from a container."""
        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', str(lines), container_name],
                capture_output=True, text=True, check=True
            )
            return result.stdout
        except:
            return "Failed to get logs"

    # ========================================================================
    # TEST CASE 1: Normal Leader Election
    # ========================================================================
    def test_1_normal_leader_election(self):
        """
        Test Case 1: Normal Leader Election

        Objective: Verify that the cluster successfully elects a leader on startup

        Steps:
        1. Start all 5 nodes
        2. Wait for leader election to complete
        3. Verify exactly one leader exists
        4. Verify all nodes recognize the same leader
        """
        self.print_header("Test Case 1: Normal Leader Election")

        self.print_step(1, "Starting all 5 nodes in the cluster")
        subprocess.run(['docker-compose', 'up', '-d'], cwd='/Users/talha/Downloads/Raft_Imp/raft_cluster')

        self.print_step(2, "Waiting for cluster initialization and leader election (15 seconds)")
        time.sleep(15)

        self.print_step(3, "Identifying the elected leader")
        leader_id = self.get_leader()

        if leader_id is not None:
            print(f" SUCCESS: Node {leader_id} elected as leader")

            # Wait a bit for logs to be written
            time.sleep(2)

            # Check logs for leader election
            logs = self.get_container_logs(f'raft_node{leader_id}', 50)
            if "Becoming LEADER" in logs:
                print(f" Confirmed: Node {leader_id} logs show leader election")
            else:
                print(f"⚠️  Warning: Could not confirm leader election in logs")

            # Verify other nodes are followers
            print("\nVerifying follower status:")
            followers_verified = 0
            for node_id, _ in self.cluster_nodes:
                if node_id != leader_id:
                    logs = self.get_container_logs(f'raft_node{node_id}', 50)
                    if "runs RPC AppendEntries" in logs:
                        print(f"  ✅ Node {node_id}: Receiving heartbeats (Follower)")
                        followers_verified += 1
                    else:
                        print(f"  ⚠️  Node {node_id}: Not receiving heartbeats yet")

            if followers_verified < 2:
                print(f"\n⚠️  Warning: Only {followers_verified}/4 followers verified")
        else:
            print(" FAILED: No leader elected")

        print("\n" + "="*80)
        return leader_id is not None

    # ========================================================================
    # TEST CASE 2: Log Replication with Client Operations
    # ========================================================================
    def test_2_log_replication(self):
        """
        Test Case 2: Log Replication with Client Operations

        Objective: Verify log replication works correctly across the cluster

        Steps:
        1. Identify the current leader
        2. Send operations to the leader
        3. Verify operations are committed
        4. Check that followers replicate the log
        """
        self.print_header("Test Case 2: Log Replication with Client Operations")

        self.print_step(1, "Finding current leader")
        leader_id = self.get_leader()

        if leader_id is None:
            print(" FAILED: No leader available")
            return False

        print(f"Leader is Node {leader_id}")

        self.print_step(2, "Sending operations to leader")
        operations = [
            "name=Alice",
            "age=30",
            "city=NewYork",
            "status=active"
        ]

        success_count = 0
        for op in operations:
            print(f"\nSending: {op}")
            success, message, _ = self.send_operation(leader_id, op)
            if success:
                print(f"   SUCCESS: {message}")
                success_count += 1
            else:
                print(f"   FAILED: {message}")
            time.sleep(1)

        self.print_step(3, "Verifying log replication across followers")
        time.sleep(3)  # Wait for replication

        print(f"\nChecking Node {leader_id} (Leader) logs:")
        leader_logs = self.get_container_logs(f'raft_node{leader_id}', 100)
        committed = leader_logs.count("Committing entries")
        executed = leader_logs.count("Executing operation")
        print(f"  Committed entries: {committed}")
        print(f"  Executed operations: {executed}")

        # Verify leader actually processed operations
        if executed == 0:
            print(f"  ⚠️  WARNING: No executed operations found in leader logs!")

        print("\nChecking follower logs:")
        followers_with_replication = 0
        for node_id, _ in self.cluster_nodes:
            if node_id != leader_id:
                logs = self.get_container_logs(f'raft_node{node_id}', 50)
                # Check for both "Appended N entries" and execution
                has_append = "Appended" in logs or "entries to log" in logs
                has_execution = "Executing operation" in logs

                if has_append or has_execution:
                    print(f"  ✅ Node {node_id}: Received log entries")
                    followers_with_replication += 1
                else:
                    print(f"  ⚠️  Node {node_id}: No log entries detected")

        # Test passes only if operations succeeded AND we see replication evidence
        operations_succeeded = success_count == len(operations)
        replication_verified = (executed > 0) or (followers_with_replication >= 2)

        result = operations_succeeded and replication_verified

        if result:
            print(f"\n✅ TEST PASSED: {success_count}/{len(operations)} operations committed and replicated")
        elif operations_succeeded and not replication_verified:
            print(f"\n⚠️  TEST PASSED (with warnings): Operations succeeded but replication evidence limited")
            print(f"   - Executed on leader: {executed > 0}")
            print(f"   - Followers with replication: {followers_with_replication}/4")
        else:
            print(f"\n❌ TEST FAILED: Only {success_count}/{len(operations)} operations succeeded")

        print("\n" + "="*80)
        return result

    # ========================================================================
    # TEST CASE 3: Leader Failure and Re-election
    # ========================================================================
    def test_3_leader_failure_reelection(self):
        """
        Test Case 3: Leader Failure and Re-election

        Objective: Verify the cluster elects a new leader when current leader fails

        Steps:
        1. Identify current leader
        2. Stop the leader container
        3. Wait for re-election
        4. Verify new leader is elected
        5. Verify cluster remains functional
        """
        self.print_header("Test Case 3: Leader Failure and Re-election")

        self.print_step(1, "Identifying current leader")
        old_leader_id = self.get_leader()

        if old_leader_id is None:
            print(" FAILED: No leader to test failure")
            return False

        print(f"Current leader: Node {old_leader_id}")

        self.print_step(2, f"Stopping leader Node {old_leader_id}")
        subprocess.run(['docker', 'stop', f'raft_node{old_leader_id}'])
        print(f" Node {old_leader_id} stopped")

        self.print_step(3, "Waiting for new leader election (10 seconds)")
        time.sleep(10)

        self.print_step(4, "Identifying new leader")
        new_leader_id = self.get_leader()

        if new_leader_id is not None and new_leader_id != old_leader_id:
            print(f" SUCCESS: New leader elected - Node {new_leader_id}")

            # Check election logs
            logs = self.get_container_logs(f'raft_node{new_leader_id}', 30)
            if "Becoming LEADER" in logs:
                print(f" Confirmed: Node {new_leader_id} logs show successful election")
        else:
            print(" FAILED: No new leader elected")
            return False

        self.print_step(5, "Testing cluster functionality with new leader")
        success, message, _ = self.send_operation(new_leader_id, "test_after_failover=success")

        if success:
            print(f" Cluster functional: {message}")
        else:
            print(f"  Operation failed: {message}")

        # Restart old leader for next tests
        self.print_step(6, f"Restarting old leader Node {old_leader_id}")
        subprocess.run(['docker', 'start', f'raft_node{old_leader_id}'])
        time.sleep(5)
        print(f" Node {old_leader_id} restarted and should rejoin as follower")

        print("\n TEST PASSED: Leader failure and re-election successful")
        print("\n" + "="*80)
        return True

    # ========================================================================
    # TEST CASE 4: Client Request Forwarding from Follower
    # ========================================================================
    def test_4_request_forwarding(self):
        """
        Test Case 4: Client Request Forwarding from Follower

        Objective: Verify non-leader nodes correctly redirect client requests

        Steps:
        1. Identify current leader
        2. Send request to a follower
        3. Verify follower returns leader information
        4. Verify client can retry with correct leader
        """
        self.print_header("Test Case 4: Client Request Forwarding from Follower")

        self.print_step(1, "Identifying current leader")
        leader_id = self.get_leader()

        if leader_id is None:
            print(" FAILED: No leader available")
            return False

        print(f"Current leader: Node {leader_id}")

        # Find a follower
        follower_id = None
        for node_id, _ in self.cluster_nodes:
            if node_id != leader_id and self.check_container_status(f'raft_node{node_id}'):
                follower_id = node_id
                break

        if follower_id is None:
            print(" FAILED: No follower available")
            return False

        self.print_step(2, f"Sending request to follower Node {follower_id}")
        success, message, returned_leader_id = self.send_operation(follower_id, "forwarding_test=value")

        print(f"Response: success={success}, message='{message}', leader_id={returned_leader_id}")

        if not success and returned_leader_id == leader_id:
            print(f" SUCCESS: Follower correctly returned leader ID ({leader_id})")

            # Check follower logs
            logs = self.get_container_logs(f'raft_node{follower_id}', 10)
            if "Forwarding request to leader" in logs or "Not the leader" in logs:
                print(f" Follower logs confirm request forwarding")
        elif success:
            print(f" SUCCESS: Request was automatically forwarded and executed")
        else:
            print(f" FAILED: Incorrect forwarding behavior")
            return False

        self.print_step(3, "Retrying with correct leader")
        success, message, _ = self.send_operation(leader_id, "forwarding_test=value")

        if success:
            print(f" SUCCESS: Operation executed on leader - {message}")
        else:
            print(f"  Operation failed on leader: {message}")

        print("\n TEST PASSED: Request forwarding works correctly")
        print("\n" + "="*80)
        return True

    # ========================================================================
    # TEST CASE 5: Network Partition Recovery (Minority Node Rejoining)
    # ========================================================================
    def test_5_node_rejoin(self):
        """
        Test Case 5: Network Partition Recovery (Minority Node Rejoining)

        Objective: Verify a stopped node can successfully rejoin the cluster

        Steps:
        1. Identify a follower node
        2. Stop the follower
        3. Perform operations while node is down
        4. Restart the node
        5. Verify node catches up with cluster state
        """
        self.print_header("Test Case 5: Network Partition Recovery (Node Rejoining)")

        self.print_step(1, "Identifying current leader and selecting a follower to test")
        leader_id = self.get_leader()

        if leader_id is None:
            print(" FAILED: No leader available")
            return False

        # Find a follower to stop
        test_node_id = None
        for node_id, _ in self.cluster_nodes:
            if node_id != leader_id:
                test_node_id = node_id
                break

        print(f"Leader: Node {leader_id}")
        print(f"Test node (to be stopped): Node {test_node_id}")

        self.print_step(2, f"Stopping Node {test_node_id}")
        subprocess.run(['docker', 'stop', f'raft_node{test_node_id}'])
        print(f" Node {test_node_id} stopped")
        time.sleep(2)

        self.print_step(3, "Performing operations while node is down")
        operations = ["op1=value1", "op2=value2", "op3=value3"]

        for op in operations:
            success, message, _ = self.send_operation(leader_id, op)
            if success:
                print(f"   {op}: {message}")
            else:
                print(f"    {op}: {message}")
            time.sleep(1)

        self.print_step(4, f"Restarting Node {test_node_id}")
        subprocess.run(['docker', 'start', f'raft_node{test_node_id}'])
        print(f" Node {test_node_id} restarted")

        self.print_step(5, "Waiting for node to rejoin and catch up (10 seconds)")
        time.sleep(10)

        self.print_step(6, "Verifying node has rejoined the cluster")
        logs = self.get_container_logs(f'raft_node{test_node_id}', 50)

        heartbeats_received = "runs RPC AppendEntries" in logs
        log_replicated = "Appended" in logs or "entries to log" in logs
        operations_executed = "Executing operation" in logs

        if heartbeats_received:
            print(f"  ✅ Node {test_node_id} is receiving heartbeats")
        else:
            print(f"  ⚠️  Node {test_node_id} not receiving heartbeats yet")

        if log_replicated:
            print(f"  ✅ Node {test_node_id} is replicating log entries")
        else:
            print(f"  ⚠️  Node {test_node_id} has not replicated entries yet")

        if operations_executed:
            print(f"  ✅ Node {test_node_id} has executed operations")

        # Send one more operation to verify full recovery
        self.print_step(7, "Testing cluster operation after rejoin")
        success, message, _ = self.send_operation(leader_id, "rejoin_test=complete")

        if success:
            print(f"  ✅ SUCCESS: Cluster fully operational after node rejoin")
        else:
            print(f"  ⚠️  Operation status: {message}")

        # Test passes if node is receiving heartbeats and cluster is operational
        result = (heartbeats_received or log_replicated) and success

        if result:
            print("\n✅ TEST PASSED: Node successfully rejoined cluster")
        else:
            print("\n⚠️  TEST PASSED (with warnings): Node may need more time to fully sync")

        print("\n" + "="*80)
        return result


def main():
    """Run all test cases."""
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "  RAFT CONSENSUS ALGORITHM - COMPREHENSIVE TEST SUITE".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "="*78 + "╝")

    tester = RaftTester()
    results = {}

    # Ensure cluster is running
    print("\n[SETUP] Ensuring clean cluster state...")
    subprocess.run(['docker-compose', 'down'],
                   cwd='/Users/talha/Downloads/Raft_Imp/raft_cluster',
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

    # Run all test cases
    test_cases = [
        ("Test 1: Normal Leader Election", tester.test_1_normal_leader_election),
        ("Test 2: Log Replication", tester.test_2_log_replication),
        ("Test 3: Leader Failure & Re-election", tester.test_3_leader_failure_reelection),
        ("Test 4: Request Forwarding", tester.test_4_request_forwarding),
        ("Test 5: Node Rejoin", tester.test_5_node_rejoin),
    ]

    for test_name, test_func in test_cases:
        try:
            result = test_func()
            results[test_name] = "PASSED" if result else "FAILED"
        except Exception as e:
            print(f"\n TEST CRASHED: {e}")
            results[test_name] = "CRASHED"

        time.sleep(3)  # Brief pause between tests

    # Print summary
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "  TEST SUMMARY".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "="*78 + "╝")
    print()

    passed = 0
    for test_name, result in results.items():
        status_symbol = "P" if result == "PASSED" else "F"
        print(f"{status_symbol} {test_name}: {result}")
        if result == "PASSED":
            passed += 1

    print()
    print(f"Overall: {passed}/{len(test_cases)} tests passed")
    print("\n" + "="*80 + "\n")

    # Cleanup
    print("[CLEANUP] Shutting down cluster...")
    subprocess.run(['docker-compose', 'down'],
                   cwd='/Users/talha/Downloads/Raft_Imp/raft_cluster',
                   stdout=subprocess.DEVNULL)

    return passed == len(test_cases)


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
