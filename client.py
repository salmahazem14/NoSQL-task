#!/usr/bin/env python3
"""
Client library for the distributed key-value store.
"""
import socket
import json
from typing import List, Tuple, Optional


class KVStoreClient:
    """Client for connecting to the key-value store server."""
    
    def __init__(self, host: str = 'localhost', port: int = 5000, timeout: float = 5.0):
        """
        Initialize client.
        
        Args:
            host: Server hostname
            port: Server port
            timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
    
    def _send_request(self, request: dict) -> dict:
        """Send request to server and get response."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(self.timeout)
                s.connect((self.host, self.port))
                
                # Send request
                message = json.dumps(request) + '\n'
                s.sendall(message.encode())
                
                # Receive response
                buffer = b''
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    buffer += chunk
                    if b'\n' in buffer:
                        break
                
                response = json.loads(buffer.decode().strip())
                return response
        except socket.timeout:
            raise TimeoutError(f"Request timed out after {self.timeout} seconds")
        except ConnectionRefusedError:
            raise ConnectionError(f"Could not connect to {self.host}:{self.port}")
        except Exception as e:
            raise RuntimeError(f"Error communicating with server: {e}")
    
    def Set(self, key: str, value: str) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: The key to set
            value: The value to store
            
        Returns:
            True if successful, False otherwise
        """
        request = {
            'command': 'set',
            'key': key,
            'value': value
        }
        response = self._send_request(request)
        return response.get('status') == 'ok'
    
    def Get(self, key: str) -> Optional[str]:
        """
        Get the value for a key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        request = {
            'command': 'get',
            'key': key
        }
        response = self._send_request(request)
        return response.get('value')
    
    def Delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful, False otherwise
        """
        request = {
            'command': 'delete',
            'key': key
        }
        response = self._send_request(request)
        return response.get('status') == 'ok'
    
    def BulkSet(self, items: List[Tuple[str, str]]) -> bool:
        """
        Set multiple key-value pairs atomically.
        
        Args:
            items: List of (key, value) tuples
            
        Returns:
            True if successful, False otherwise
        """
        request = {
            'command': 'bulk_set',
            'items': items
        }
        response = self._send_request(request)
        return response.get('status') == 'ok'
    
    def SearchText(self, query: str) -> List[str]:
        """
        Full-text search for keys containing query words.
        
        Args:
            query: Search query string
            
        Returns:
            List of matching keys
        """
        request = {
            'command': 'search_text',
            'query': query
        }
        response = self._send_request(request)
        return response.get('results', [])
    
    def SearchSimilar(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Search for similar values using embeddings.
        
        Args:
            query: Query string
            top_k: Number of results to return
            
        Returns:
            List of (key, similarity_score) tuples
        """
        request = {
            'command': 'search_similar',
            'query': query,
            'top_k': top_k
        }
        response = self._send_request(request)
        return [tuple(item) for item in response.get('results', [])]
    
    def GetAllKeys(self) -> List[str]:
        """
        Get all keys in the store.
        
        Returns:
            List of all keys
        """
        request = {
            'command': 'get_all_keys'
        }
        response = self._send_request(request)
        return response.get('keys', [])


if __name__ == '__main__':
    # Example usage
    client = KVStoreClient()
    
    # Set a value
    client.Set('name', 'Alice')
    
    # Get a value
    value = client.Get('name')
    print(f"name = {value}")
    
    # Bulk set
    client.BulkSet([('key1', 'value1'), ('key2', 'value2')])
    
    # Delete
    client.Delete('key1')
    
    # Get all keys
    keys = client.GetAllKeys()
    print(f"All keys: {keys}")
