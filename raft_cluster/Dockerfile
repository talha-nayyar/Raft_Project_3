FROM python:3.9

# Disable Python output buffering for immediate logging
ENV PYTHONUNBUFFERED=1

WORKDIR /raft_cluster

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY *.proto ./

# Generate gRPC code
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

# Default command (will be overridden in docker-compose)
CMD ["python3", "-u", "raft_node.py", "0"]
