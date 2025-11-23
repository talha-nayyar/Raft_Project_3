import grpc
from concurrent import futures
import time
import random
import threading
from enum import Enum
import sys

import raft_pb2
import raft_pb2_grpc


class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id, peers):
        """
        Initialize a Raft node.

        Args:
            node_id: Unique identifier for this node
            peers: List of (node_id, address) tuples for all nodes in cluster
        """
        self.node_id = node_id
        self.peers = {peer_id: addr for peer_id, addr in peers if peer_id != node_id}

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of LogEntry objects

        # Volatile state
        self.commit_index = -1  # Index of highest log entry known to be committed
        self.last_applied = -1  # Index of highest log entry applied to state machine
        self.state = NodeState.FOLLOWER

        # Leader state (reinitialized after election)
        self.next_index = {}   # For each server, index of next log entry to send
        self.match_index = {}  # For each server, index of highest log entry known to be replicated

        # Timing
        self.heartbeat_timeout = 1.0  # 1 second
        self.election_timeout = None
        self.last_heartbeat = time.time()

        # State machine (simple key-value store for demonstration)
        self.state_machine = {}

        # Locks
        self.lock = threading.Lock()

        # Leader info
        self.current_leader = None

        # Start background threads
        self.running = True
        self.election_timer_started = False
        # Don't start election timer yet - will be started after peers are ready
        threading.Thread(target=self._election_timer, daemon=True).start()

        print(f"Node {self.node_id} initialized as FOLLOWER")

    def reset_election_timeout(self):
        """Reset election timeout to random value between 1.5 and 3 seconds."""
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()

    def _election_timer(self):
        """Background thread that monitors election timeout."""
        while self.running:
            time.sleep(0.1)  # Check every 100ms

            # Don't start elections until timer is explicitly started
            if not self.election_timer_started:
                continue

            should_start_election = False
            with self.lock:
                if self.state == NodeState.LEADER:
                    # Leader sends heartbeats
                    continue

                elapsed = time.time() - self.last_heartbeat
                if elapsed >= self.election_timeout:
                    should_start_election = True

            # Start election outside the lock to avoid deadlock
            if should_start_election:
                print(f"Node {self.node_id}: Election timeout! Starting election...")
                self._start_election()

    def _start_election(self):
        """Start a new election."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.reset_election_timeout()

        term = self.current_term
        votes_received = 1  # Vote for self

        print(f"Node {self.node_id}: Starting election for term {term}")

        # Request votes from all peers
        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1].term if self.log else 0

        # Create threads to request votes in parallel
        def request_vote_from_peer(peer_id, peer_addr):
            nonlocal votes_received
            try:
                print(f"Node {self.node_id} sends RPC RequestVote to Node {peer_id}")

                channel = grpc.insecure_channel(peer_addr)
                stub = raft_pb2_grpc.RaftNodeStub(channel)

                request = raft_pb2.RequestVoteRequest(
                    term=term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term
                )

                response = stub.RequestVote(request, timeout=1.0)

                with self.lock:
                    if response.term > self.current_term:
                        self._become_follower(response.term)
                    elif response.vote_granted and self.state == NodeState.CANDIDATE and self.current_term == term:
                        votes_received += 1
                        print(f"Node {self.node_id}: Received vote from Node {peer_id} (total: {votes_received}/{len(self.peers) + 1})")

                        # Check if we have majority
                        if votes_received > (len(self.peers) + 1) // 2:
                            self._become_leader()

                channel.close()
            except Exception as e:
                print(f"Node {self.node_id}: Error requesting vote from Node {peer_id}: {e}")

        # Request votes from all peers in parallel
        threads = []
        for peer_id, peer_addr in self.peers.items():
            thread = threading.Thread(target=request_vote_from_peer, args=(peer_id, peer_addr))
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

    def _become_follower(self, term):
        """Transition to follower state."""
        print(f"Node {self.node_id}: Becoming FOLLOWER for term {term}")
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.reset_election_timeout()

    def _become_leader(self):
        """Transition to leader state."""
        if self.state != NodeState.CANDIDATE:
            return

        print(f"Node {self.node_id}: Becoming LEADER for term {self.current_term}")
        self.state = NodeState.LEADER
        self.current_leader = self.node_id

        # Initialize leader state
        for peer_id in self.peers:
            self.next_index[peer_id] = len(self.log)
            self.match_index[peer_id] = -1

        # Start sending heartbeats
        threading.Thread(target=self._send_heartbeats, daemon=True).start()

    def _send_heartbeats(self):
        """Send periodic heartbeats to all followers."""
        while self.running and self.state == NodeState.LEADER:
            with self.lock:
                term = self.current_term
                leader_id = self.node_id
                commit_index = self.commit_index

            # Send heartbeat to all peers
            for peer_id, peer_addr in self.peers.items():
                threading.Thread(
                    target=self._send_append_entries,
                    args=(peer_id, peer_addr, term, leader_id, commit_index),
                    daemon=True
                ).start()

            time.sleep(self.heartbeat_timeout)

    def _send_append_entries(self, peer_id, peer_addr, term, leader_id, leader_commit):
        """Send AppendEntries RPC to a specific peer."""
        try:
            with self.lock:
                next_idx = self.next_index.get(peer_id, len(self.log))
                prev_log_index = next_idx - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0

                # Get entries to send
                entries = []
                if next_idx < len(self.log):
                    # Send log entries
                    for i in range(next_idx, len(self.log)):
                        entries.append(raft_pb2.LogEntry(
                            term=self.log[i].term,
                            index=self.log[i].index,
                            operation=self.log[i].operation
                        ))

            print(f"Node {self.node_id} sends RPC AppendEntries to Node {peer_id}")

            channel = grpc.insecure_channel(peer_addr)
            stub = raft_pb2_grpc.RaftNodeStub(channel)

            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=leader_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=leader_commit
            )

            response = stub.AppendEntries(request, timeout=1.0)

            with self.lock:
                if response.term > self.current_term:
                    self._become_follower(response.term)
                elif self.state == NodeState.LEADER and self.current_term == term:
                    if response.success:
                        # Update next_index and match_index
                        if entries:
                            self.match_index[peer_id] = prev_log_index + len(entries)
                            self.next_index[peer_id] = self.match_index[peer_id] + 1

                        # Check if we can commit more entries
                        self._update_commit_index()
                    else:
                        # Decrement next_index and retry
                        self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)

            channel.close()
        except Exception as e:
            print(f"Node {self.node_id}: Error sending AppendEntries to Node {peer_id}: {e}")

    def _update_commit_index(self):
        """Update commit index based on match_index of followers."""
        if self.state != NodeState.LEADER:
            return

        # Find the highest index that has been replicated on a majority
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term == self.current_term:
                # Count how many servers have this entry
                count = 1  # Leader has it
                for peer_id in self.peers:
                    if self.match_index.get(peer_id, -1) >= n:
                        count += 1

                # Check if majority
                if count > (len(self.peers) + 1) // 2:
                    print(f"Node {self.node_id}: Committing entries up to index {n}")
                    self.commit_index = n
                    self._apply_committed_entries()
                    break

    def _apply_committed_entries(self):
        """Apply committed but not yet applied log entries to state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            print(f"Node {self.node_id}: Executing operation: {entry.operation}")

            # Simple key-value store operations
            try:
                if '=' in entry.operation:
                    # SET operation: key=value
                    key, value = entry.operation.split('=', 1)
                    self.state_machine[key.strip()] = value.strip()
                    print(f"Node {self.node_id}: State machine updated: {key.strip()} = {value.strip()}")
            except Exception as e:
                print(f"Node {self.node_id}: Error applying operation: {e}")

    # RPC Handlers

    def RequestVote(self, request, context):
        """Handle RequestVote RPC."""
        print(f"Node {self.node_id} runs RPC RequestVote called by Node {request.candidate_id}")

        with self.lock:
            # If request term is greater, update our term
            if request.term > self.current_term:
                self._become_follower(request.term)

            vote_granted = False

            # Grant vote if:
            # 1. Request term >= our term
            # 2. We haven't voted for anyone else in this term
            # 3. Candidate's log is at least as up-to-date as ours
            if request.term >= self.current_term:
                if self.voted_for is None or self.voted_for == request.candidate_id:
                    # Check if candidate's log is up-to-date
                    last_log_index = len(self.log) - 1
                    last_log_term = self.log[-1].term if self.log else 0

                    log_ok = (request.last_log_term > last_log_term or
                             (request.last_log_term == last_log_term and
                              request.last_log_index >= last_log_index))

                    if log_ok:
                        vote_granted = True
                        self.voted_for = request.candidate_id
                        self.reset_election_timeout()
                        print(f"Node {self.node_id}: Granted vote to Node {request.candidate_id} for term {request.term}")

            return raft_pb2.RequestVoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )

    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC."""
        print(f"Node {self.node_id} runs RPC AppendEntries called by Node {request.leader_id}")

        with self.lock:
            # If request term is greater, update our term
            if request.term > self.current_term:
                self._become_follower(request.term)

            success = False

            if request.term >= self.current_term:
                # Valid leader
                self.state = NodeState.FOLLOWER
                self.current_leader = request.leader_id
                self.reset_election_timeout()

                # Check if our log matches leader's at prev_log_index
                if request.prev_log_index == -1:
                    # No previous entry required
                    success = True
                elif request.prev_log_index < len(self.log):
                    if self.log[request.prev_log_index].term == request.prev_log_term:
                        success = True

                if success and request.entries:
                    # Append new entries
                    # First, remove any conflicting entries
                    insert_idx = request.prev_log_index + 1

                    for i, entry in enumerate(request.entries):
                        idx = insert_idx + i
                        if idx < len(self.log):
                            # Check for conflict
                            if self.log[idx].term != entry.term:
                                # Remove this and all following entries
                                self.log = self.log[:idx]
                                self.log.append(entry)
                        else:
                            # Append new entry
                            self.log.append(entry)

                    print(f"Node {self.node_id}: Appended {len(request.entries)} entries to log")

                # Update commit index
                if request.leader_commit > self.commit_index:
                    self.commit_index = min(request.leader_commit, len(self.log) - 1)
                    self._apply_committed_entries()

            return raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=success
            )

    def ExecuteOperation(self, request, context):
        """Handle client operation request."""
        print(f"Node {self.node_id}: Received client request: {request.operation}")

        with self.lock:
            if self.state != NodeState.LEADER:
                # Forward to leader
                if self.current_leader is not None:
                    print(f"Node {self.node_id}: Forwarding request to leader (Node {self.current_leader})")
                    return raft_pb2.ClientResponse(
                        success=False,
                        message="Not the leader. Forwarding to leader.",
                        leader_id=self.current_leader
                    )
                else:
                    return raft_pb2.ClientResponse(
                        success=False,
                        message="No leader elected yet.",
                        leader_id=-1
                    )

            # Leader handles the request
            # Append to log
            entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log),
                operation=request.operation
            )
            self.log.append(entry)

            print(f"Node {self.node_id}: Appended operation to log at index {entry.index}")

        # Wait for operation to be committed (with timeout)
        start_time = time.time()
        timeout = 5.0

        while time.time() - start_time < timeout:
            with self.lock:
                if self.commit_index >= entry.index:
                    return raft_pb2.ClientResponse(
                        success=True,
                        message=f"Operation committed and executed on leader",
                        leader_id=self.node_id
                    )
            time.sleep(0.1)

        return raft_pb2.ClientResponse(
            success=False,
            message="Operation timeout - may not be committed",
            leader_id=self.node_id
        )


def wait_for_peers(node_id, peers, max_wait=30):
    """Wait for peer nodes to be ready before starting election timer."""
    print(f"Node {node_id}: Waiting for peer nodes to be ready...")
    start_time = time.time()

    ready_peers = set()
    all_peer_ids = set(peer_id for peer_id, _ in peers if peer_id != node_id)

    while time.time() - start_time < max_wait:
        for peer_id, peer_addr in peers:
            if peer_id == node_id or peer_id in ready_peers:
                continue

            try:
                # Try to connect to the peer
                channel = grpc.insecure_channel(peer_addr)
                grpc.channel_ready_future(channel).result(timeout=1)
                ready_peers.add(peer_id)
                print(f"Node {node_id}: Peer Node {peer_id} is ready")
                channel.close()
            except:
                # Peer not ready yet
                pass

        # Check if majority of peers are ready (including self)
        if len(ready_peers) >= len(all_peer_ids) // 2:
            print(f"Node {node_id}: Majority of peers are ready ({len(ready_peers)}/{len(all_peer_ids)})")
            return True

        time.sleep(0.5)

    # Even if not all peers are ready, continue if we waited long enough
    print(f"Node {node_id}: Starting with {len(ready_peers)}/{len(all_peer_ids)} peers ready")
    return True


def serve(node_id, port, peers):
    """Start the Raft node server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_node = RaftNode(node_id, peers)
    raft_pb2_grpc.add_RaftNodeServicer_to_server(raft_node, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Node {node_id} server running on port {port}")

    # Wait for peers in a background thread so server can handle RPCs
    def start_election_timer_when_ready():
        wait_for_peers(node_id, peers)
        # Now enable and reset the election timer to start elections
        raft_node.election_timer_started = True
        raft_node.reset_election_timeout()
        print(f"Node {node_id}: Election timer started")

    threading.Thread(target=start_election_timer_when_ready, daemon=True).start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        raft_node.running = False
        server.stop(0)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python raft_node.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])

    # Default cluster configuration (5 nodes)
    peers = [
        (0, 'node0:50060'),
        (1, 'node1:50061'),
        (2, 'node2:50062'),
        (3, 'node3:50063'),
        (4, 'node4:50064')
    ]

    port = 50060 + node_id
    serve(node_id, port, peers)
