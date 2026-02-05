#!/usr/bin/env python3
"""
Distributed Key-Value Store Server with ACID guarantees and replication.
"""
import socket
import threading
import json
import os
import sys
import time
import pickle
import random
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from enum import Enum

# Import platform-specific modules
if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl


class NodeRole(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    CANDIDATE = "candidate"


class KVStore:
    """Core key-value storage engine with WAL and ACID guarantees."""
    
    def __init__(self, data_dir: str, debug_mode: bool = False):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self._data_file = self.data_dir / "data.json"
        self._wal_file = self.data_dir / "wal.log"
        self._lock_file = self.data_dir / "store.lock"
        
        self._data: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._file_lock = None
        self.debug_mode = debug_mode
        
        # Indexing structures
        self._inverted_index: Dict[str, set] = {}  # word -> set of keys
        self._embeddings: Dict[str, List[float]] = {}  # key -> embedding vector
        
        self._acquire_file_lock()
        self._recover_from_wal()
        self._load_data()
    
    def _acquire_file_lock(self):
        """Acquire exclusive file lock to prevent multiple instances."""
        self._file_lock = open(self._lock_file, 'w')
        try:
            if sys.platform == 'win32':
                # Windows file locking
                msvcrt.locking(self._file_lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                # Unix file locking
                fcntl.flock(self._file_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            raise RuntimeError("Another instance is already running")
    
    def _write_wal(self, operation: str, key: str, value: Optional[str] = None):
        """Write operation to WAL (synchronous, always succeeds)."""
        entry = {
            'op': operation,
            'key': key,
            'value': value,
            'timestamp': time.time()
        }
        with self._lock:
            # WAL writes are always synchronous (no debug mode skip)
            with open(self._wal_file, 'a') as f:
                f.write(json.dumps(entry) + '\n')
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
    
    def _write_wal_bulk(self, operations: List[Tuple[str, str]]):
        """Write bulk operation to WAL atomically."""
        entries = [{
            'op': 'bulk_set',
            'operations': operations,
            'timestamp': time.time()
        }]
        with self._lock:
            with open(self._wal_file, 'a') as f:
                for entry in entries:
                    f.write(json.dumps(entry) + '\n')
                f.flush()
                os.fsync(f.fileno())
    
    def _recover_from_wal(self):
        """Recover state from WAL after crash."""
        if not self._wal_file.exists():
            return
        
        with self._lock:
            with open(self._wal_file, 'r') as f:
                for line in f:
                    if not line.strip():
                        continue
                    entry = json.loads(line)
                    
                    if entry['op'] == 'set':
                        self._data[entry['key']] = entry['value']
                    elif entry['op'] == 'delete':
                        self._data.pop(entry['key'], None)
                    elif entry['op'] == 'bulk_set':
                        for key, value in entry['operations']:
                            self._data[key] = value
    
    def _load_data(self):
        """Load data from persistent storage."""
        if self._data_file.exists():
            with self._lock:
                with open(self._data_file, 'r') as f:
                    self._data.update(json.load(f))
    
    def _save_data(self):
        """Save data to disk (can be skipped in debug mode)."""
        with self._lock:
            if self.debug_mode and random.random() < 0.01:
                return  # Simulate filesystem sync issues
            
            # Atomic write: write to temp file, then rename
            temp_file = self._data_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(self._data, f)
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(temp_file, self._data_file)
    
    def _clear_wal(self):
        """Clear WAL after successful checkpoint."""
        with self._lock:
            if self._wal_file.exists():
                self._wal_file.unlink()
    
    def _update_inverted_index(self, key: str, value: str):
        """Update inverted index for full-text search."""
        # Remove old entries for this key
        for word_set in self._inverted_index.values():
            word_set.discard(key)
        
        # Add new entries
        words = value.lower().split()
        for word in words:
            if word not in self._inverted_index:
                self._inverted_index[word] = set()
            self._inverted_index[word].add(key)
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate simple word embedding (bag of words + hash-based)."""
        # Simple embedding: hash-based features
        embedding = [0.0] * 128
        words = text.lower().split()
        
        for word in words:
            # Use hash to determine feature positions
            for i in range(5):  # Multiple hash functions
                idx = hash(word + str(i)) % 128
                embedding[idx] += 1.0
        
        # Normalize
        norm = sum(x*x for x in embedding) ** 0.5
        if norm > 0:
            embedding = [x / norm for x in embedding]
        
        return embedding
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        return sum(a * b for a, b in zip(vec1, vec2))
    
    def set(self, key: str, value: str) -> bool:
        """Set a key-value pair with ACID guarantees."""
        with self._lock:
            self._write_wal('set', key, value)
            self._data[key] = value
            self._update_inverted_index(key, value)
            self._embeddings[key] = self._generate_embedding(value)
            self._save_data()
            return True
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key."""
        with self._lock:
            return self._data.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete a key."""
        with self._lock:
            if key not in self._data:
                return False
            
            self._write_wal('delete', key)
            del self._data[key]
            
            # Clean up indexes
            for word_set in self._inverted_index.values():
                word_set.discard(key)
            self._embeddings.pop(key, None)
            
            self._save_data()
            return True
    
    def bulk_set(self, items: List[Tuple[str, str]]) -> bool:
        """Atomically set multiple key-value pairs."""
        with self._lock:
            # Write entire operation to WAL atomically
            self._write_wal_bulk(items)
            
            # Apply all changes
            for key, value in items:
                self._data[key] = value
                self._update_inverted_index(key, value)
                self._embeddings[key] = self._generate_embedding(value)
            
            self._save_data()
            return True
    
    def search_text(self, query: str) -> List[str]:
        """Full-text search using inverted index."""
        with self._lock:
            words = query.lower().split()
            if not words:
                return []
            
            # Find keys containing all query words
            result_sets = [self._inverted_index.get(word, set()) for word in words]
            if not result_sets:
                return []
            
            # Intersection of all sets
            results = set.intersection(*result_sets) if result_sets else set()
            return list(results)
    
    def search_similar(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Search for similar values using embeddings."""
        query_embedding = self._generate_embedding(query)
        
        with self._lock:
            similarities = []
            for key, embedding in self._embeddings.items():
                sim = self._cosine_similarity(query_embedding, embedding)
                similarities.append((key, sim))
            
            # Sort by similarity and return top k
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:top_k]
    
    def checkpoint(self):
        """Checkpoint: save data and clear WAL."""
        with self._lock:
            self._save_data()
            self._clear_wal()
    
    def get_all_keys(self) -> List[str]:
        """Get all keys in the store."""
        with self._lock:
            return list(self._data.keys())
    
    def close(self):
        """Gracefully close the store."""
        self.checkpoint()
        if self._file_lock:
            if sys.platform == 'win32':
                try:
                    msvcrt.locking(self._file_lock.fileno(), msvcrt.LK_UNLCK, 1)
                except:
                    pass
            else:
                fcntl.flock(self._file_lock.fileno(), fcntl.LOCK_UN)
            self._file_lock.close()


class ReplicationManager:
    """Manages replication between primary and secondary nodes."""
    
    def __init__(self, node_id: str, peers: List[Tuple[str, int]], 
                 role: NodeRole = NodeRole.SECONDARY):
        self.node_id = node_id
        self.peers = peers
        self.role = role
        self.term = 0
        self.voted_for = None
        self.last_heartbeat = time.time()
        self.votes_received = set()
        self.lock = threading.Lock()
        
        # Replication state
        self.replicated_operations = []
        self.commit_index = 0
    
    def start_election(self):
        """Start leader election."""
        with self.lock:
            self.role = NodeRole.CANDIDATE
            self.term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
    
    def receive_vote(self, voter_id: str):
        """Record a vote."""
        with self.lock:
            self.votes_received.add(voter_id)
            # Majority needed (2 out of 3)
            if len(self.votes_received) >= 2:
                self.role = NodeRole.PRIMARY
                return True
        return False
    
    def step_down(self):
        """Step down from primary role."""
        with self.lock:
            self.role = NodeRole.SECONDARY
            self.voted_for = None
    
    def update_heartbeat(self):
        """Update last heartbeat time."""
        self.last_heartbeat = time.time()
    
    def is_heartbeat_timeout(self, timeout: float = 5.0) -> bool:
        """Check if heartbeat has timed out."""
        return time.time() - self.last_heartbeat > timeout


class KVStoreServer:
    """TCP server for the key-value store with replication support."""
    
    def __init__(self, host: str, port: int, data_dir: str, 
                 node_id: str = "node1", peers: List[Tuple[str, int]] = None,
                 is_primary: bool = True, debug_mode: bool = False):
        self.host = host
        self.port = port
        self.store = KVStore(data_dir, debug_mode=debug_mode)
        self.running = False
        self.server_socket = None
        
        # Replication
        self.node_id = node_id
        self.peers = peers or []
        self.replication_manager = ReplicationManager(
            node_id, 
            self.peers,
            NodeRole.PRIMARY if is_primary else NodeRole.SECONDARY
        )
        
        # Peer connections
        self.peer_sockets: Dict[str, socket.socket] = {}
        
    def start(self):
        """Start the server."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        # Start heartbeat/election thread if we have peers
        if self.peers:
            threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        
        print(f"Server started on {self.host}:{self.port} as {self.replication_manager.role.value}")
        
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                try:
                    client_socket, address = self.server_socket.accept()
                    threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    ).start()
                except socket.timeout:
                    continue
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection: {e}")
    
    def _heartbeat_loop(self):
        """Send heartbeats if primary, check for timeouts if secondary."""
        while self.running:
            time.sleep(1)
            
            if self.replication_manager.role == NodeRole.PRIMARY:
                self._send_heartbeats()
            elif self.replication_manager.role == NodeRole.SECONDARY:
                if self.replication_manager.is_heartbeat_timeout():
                    self._start_election()
    
    def _send_heartbeats(self):
        """Send heartbeat to all peers."""
        for peer_host, peer_port in self.peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1.0)
                    s.connect((peer_host, peer_port))
                    message = {
                        'type': 'heartbeat',
                        'term': self.replication_manager.term,
                        'node_id': self.node_id
                    }
                    s.sendall(json.dumps(message).encode() + b'\n')
            except:
                pass
    
    def _start_election(self):
        """Start leader election."""
        self.replication_manager.start_election()
        
        # Request votes from peers
        for peer_host, peer_port in self.peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1.0)
                    s.connect((peer_host, peer_port))
                    message = {
                        'type': 'vote_request',
                        'term': self.replication_manager.term,
                        'candidate_id': self.node_id
                    }
                    s.sendall(json.dumps(message).encode() + b'\n')
                    
                    response = s.recv(4096).decode()
                    if response:
                        resp_data = json.loads(response)
                        if resp_data.get('vote_granted'):
                            if self.replication_manager.receive_vote(resp_data['voter_id']):
                                print(f"Node {self.node_id} became PRIMARY")
            except:
                pass
    
    def _replicate_to_secondaries(self, operation: Dict[str, Any]):
        """Replicate operation to secondary nodes."""
        if self.replication_manager.role != NodeRole.PRIMARY:
            return
        
        for peer_host, peer_port in self.peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1.0)
                    s.connect((peer_host, peer_port))
                    operation['type'] = 'replicate'
                    s.sendall(json.dumps(operation).encode() + b'\n')
            except:
                pass
    
    def _handle_client(self, client_socket: socket.socket, address):
        """Handle client connection."""
        try:
            buffer = b''
            while self.running:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                
                buffer += chunk
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if not line:
                        continue
                    
                    try:
                        request = json.loads(line.decode())
                        response = self._process_request(request)
                        client_socket.sendall(json.dumps(response).encode() + b'\n')
                    except Exception as e:
                        error_response = {'status': 'error', 'message': str(e)}
                        client_socket.sendall(json.dumps(error_response).encode() + b'\n')
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
    
    def _process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process client request."""
        cmd = request.get('command')
        
        # Handle replication messages
        if request.get('type') == 'heartbeat':
            self.replication_manager.update_heartbeat()
            # If we get heartbeat from higher term, step down
            if request['term'] > self.replication_manager.term:
                self.replication_manager.step_down()
            return {'status': 'ok'}
        
        if request.get('type') == 'vote_request':
            # Grant vote if we haven't voted or voted for this candidate
            with self.replication_manager.lock:
                if (self.replication_manager.voted_for is None or 
                    self.replication_manager.voted_for == request['candidate_id']):
                    self.replication_manager.voted_for = request['candidate_id']
                    self.replication_manager.term = request['term']
                    return {
                        'vote_granted': True,
                        'voter_id': self.node_id,
                        'term': self.replication_manager.term
                    }
            return {'vote_granted': False}
        
        if request.get('type') == 'replicate':
            # Secondary receiving replication
            cmd = request.get('command')
            if cmd == 'set':
                self.store.set(request['key'], request['value'])
            elif cmd == 'delete':
                self.store.delete(request['key'])
            elif cmd == 'bulk_set':
                self.store.bulk_set(request['items'])
            return {'status': 'ok'}
        
        # Only primary handles client requests
        if self.replication_manager.role != NodeRole.PRIMARY and cmd in ['set', 'delete', 'bulk_set']:
            return {'status': 'error', 'message': 'Not primary node'}
        
        # Process commands
        if cmd == 'set':
            key = request.get('key')
            value = request.get('value')
            success = self.store.set(key, value)
            
            # Replicate to secondaries
            if self.peers:
                self._replicate_to_secondaries(request)
            
            return {'status': 'ok' if success else 'error'}
        
        elif cmd == 'get':
            key = request.get('key')
            value = self.store.get(key)
            return {'status': 'ok', 'value': value}
        
        elif cmd == 'delete':
            key = request.get('key')
            success = self.store.delete(key)
            
            # Replicate to secondaries
            if self.peers:
                self._replicate_to_secondaries(request)
            
            return {'status': 'ok' if success else 'error'}
        
        elif cmd == 'bulk_set':
            items = request.get('items', [])
            success = self.store.bulk_set(items)
            
            # Replicate to secondaries
            if self.peers:
                self._replicate_to_secondaries(request)
            
            return {'status': 'ok' if success else 'error'}
        
        elif cmd == 'search_text':
            query = request.get('query', '')
            results = self.store.search_text(query)
            return {'status': 'ok', 'results': results}
        
        elif cmd == 'search_similar':
            query = request.get('query', '')
            top_k = request.get('top_k', 10)
            results = self.store.search_similar(query, top_k)
            return {'status': 'ok', 'results': results}
        
        elif cmd == 'get_all_keys':
            keys = self.store.get_all_keys()
            return {'status': 'ok', 'keys': keys}
        
        else:
            return {'status': 'error', 'message': 'Unknown command'}
    
    def stop(self):
        """Stop the server gracefully."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.store.close()


if __name__ == '__main__':
    import sys
    
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    data_dir = sys.argv[2] if len(sys.argv) > 2 else f"./data_{port}"
    
    server = KVStoreServer('localhost', port, data_dir)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()
