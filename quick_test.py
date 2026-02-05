"""
Quick test script for Windows - verifies basic functionality.
"""
import subprocess
import time
import sys
import os
import shutil

PYTHON_EXE = sys.executable

def print_status(message, success=True):
    """Print status message with color."""
    symbol = "✓" if success else "✗"
    print(f"{symbol} {message}")

def cleanup():
    """Clean up test data."""
    for dir_name in ['quick_test_data', 'persist_test_data']:
        if os.path.exists(dir_name):
            try:
                shutil.rmtree(dir_name)
            except:
                pass

def kill_server(process):
    """Kill server process."""
    if sys.platform == 'win32':
        subprocess.call(['taskkill', '/F', '/T', '/PID', str(process.pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        process.terminate()
    try:
        process.wait(timeout=3)
    except:
        pass

def main():
    print("=" * 60)
    print("  Windows Quick Test - KV Store")
    print("=" * 60)
    
    cleanup()
    
    # Test 1: Basic operations
    print("\nTest 1: Basic Operations")
    print("-" * 60)
    
    print("Starting server...")
    server = subprocess.Popen(
        [PYTHON_EXE, 'server.py', '9000', './quick_test_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
    )
    time.sleep(2)
    
    try:
        from client import KVStoreClient
        client = KVStoreClient('localhost', 9000)
        
        # Test Set and Get
        client.Set('test_key', 'test_value')
        value = client.Get('test_key')
        assert value == 'test_value', f'Expected test_value, got {value}'
        print_status("Set and Get works")
        
        # Test Bulk Set
        items = [('key1', 'value1'), ('key2', 'value2'), ('key3', 'value3')]
        client.BulkSet(items)
        for key, expected_value in items:
            value = client.Get(key)
            assert value == expected_value, f'Bulk set failed for {key}'
        print_status("Bulk Set works")
        
        # Test Delete
        client.Delete('test_key')
        value = client.Get('test_key')
        assert value is None, f'Delete failed, got {value}'
        print_status("Delete works")
        
        # Test Search
        client.Set('doc1', 'hello world')
        client.Set('doc2', 'world peace')
        results = client.SearchText('world')
        assert 'doc1' in results and 'doc2' in results, f'Search failed: {results}'
        print_status("Full-text search works")
        
    except Exception as e:
        print_status(f"Test failed: {e}", success=False)
        kill_server(server)
        cleanup()
        return False
    
    kill_server(server)
    time.sleep(1)
    
    # Test 2: Persistence
    print("\nTest 2: Persistence Across Restarts")
    print("-" * 60)
    
    print("Starting server...")
    server = subprocess.Popen(
        [PYTHON_EXE, 'server.py', '9001', './persist_test_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
    )
    time.sleep(2)
    
    try:
        client = KVStoreClient('localhost', 9001)
        client.Set('persistent_key', 'persistent_value')
        print_status("Data written")
        
        print("Stopping server...")
        kill_server(server)
        time.sleep(1)
        
        print("Restarting server...")
        server = subprocess.Popen(
            [PYTHON_EXE, 'server.py', '9001', './persist_test_data'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        time.sleep(2)
        
        client = KVStoreClient('localhost', 9001)
        value = client.Get('persistent_key')
        assert value == 'persistent_value', f'Persistence failed: {value}'
        print_status("Data survived restart")
        
    except Exception as e:
        print_status(f"Test failed: {e}", success=False)
        kill_server(server)
        cleanup()
        return False
    
    kill_server(server)
    
    # Cleanup
    cleanup()
    
    print("\n" + "=" * 60)
    print("  All Quick Tests PASSED!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run full tests: python tests_windows.py")
    print("  2. Run benchmarks: python benchmarks_windows.py")
    print("  3. Start your own server: python server.py 5000 ./mydata")
    
    return True

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
