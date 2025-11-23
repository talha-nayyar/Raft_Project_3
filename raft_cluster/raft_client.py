import grpc
import sys
import time

import raft_pb2
import raft_pb2_grpc


class RaftClient:
    def __init__(self, cluster_nodes):
        """
        Initialize Raft client.

        Args:
            cluster_nodes: List of (node_id, address) tuples
        """
        self.cluster_nodes = cluster_nodes
        self.current_leader = None

    def execute_operation(self, operation):
        """
        Execute an operation on the Raft cluster.

        Args:
            operation: The operation string to execute (e.g., "x=10")

        Returns:
            Tuple of (success, message)
        """
        # Try current leader first if known
        if self.current_leader is not None:
            success, message = self._send_to_node(self.current_leader, operation)
            if success:
                return True, message

        # Try all nodes until we find the leader
        for node_id, address in self.cluster_nodes:
            print(f"Client: Trying to send request to Node {node_id}")
            success, message = self._send_to_node((node_id, address), operation)

            if success:
                self.current_leader = (node_id, address)
                return True, message

        return False, "Could not find leader or execute operation"

    def _send_to_node(self, node_info, operation):
        """
        Send operation to a specific node.

        Args:
            node_info: Tuple of (node_id, address)
            operation: The operation to execute

        Returns:
            Tuple of (success, message)
        """
        node_id, address = node_info
        try:
            channel = grpc.insecure_channel(address)
            stub = raft_pb2_grpc.RaftNodeStub(channel)

            request = raft_pb2.ClientRequest(operation=operation)
            response = stub.ExecuteOperation(request, timeout=10.0)

            channel.close()

            if response.success:
                print(f"Client: Operation executed successfully on Node {node_id}")
                return True, response.message
            else:
                print(f"Client: Node {node_id} responded: {response.message}")
                if response.leader_id != -1:
                    # Update leader info
                    for nid, addr in self.cluster_nodes:
                        if nid == response.leader_id:
                            self.current_leader = (nid, addr)
                            # Retry with leader
                            return self._send_to_node((nid, addr), operation)
                return False, response.message

        except Exception as e:
            print(f"Client: Error connecting to Node {node_id}: {e}")
            return False, str(e)


def main():
    # Cluster configuration (5 nodes)
    cluster_nodes = [
        (0, 'node0:50060'),
        (1, 'node1:50061'),
        (2, 'node2:50062'),
        (3, 'node3:50063'),
        (4, 'node4:50064')
    ]

    client = RaftClient(cluster_nodes)

    print("Raft Client Started")
    print("Commands:")
    print("  SET <key>=<value>  - Set a key-value pair")
    print("  exit               - Exit client")
    print()

    # Wait a bit for cluster to elect leader
    print("Waiting for cluster to elect leader...")
    time.sleep(5)

    while True:
        try:
            operation = input("Enter operation: ").strip()

            if operation.lower() == 'exit':
                break

            if not operation:
                continue

            success, message = client.execute_operation(operation)

            if success:
                print(f"SUCCESS: {message}")
            else:
                print(f"FAILED: {message}")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

    print("\nClient shutting down...")


if __name__ == '__main__':
    main()
