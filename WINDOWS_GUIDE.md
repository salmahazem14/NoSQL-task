# Windows Setup Guide

This guide explains how to run the distributed key-value store on Windows.

## Quick Start

### 1. Use Windows-Compatible Files

On Windows, use these files instead of the Linux versions:
- `tests_windows.py` instead of `tests.py`
- `benchmarks_windows.py` instead of `benchmarks.py`

The `server.py` and `client.py` work on both platforms.

### 2. Running Tests

```bash
python tests_windows.py
```

Expected output:
```
test_bulk_set ... ok
test_full_text_search ... ok
test_get_without_setting ... ok
test_set_then_delete_then_get ... ok
test_set_then_get ... ok
test_set_then_set_same_key_then_get ... ok
test_similarity_search ... ok
...
OK
```

### 3. Running Benchmarks

```bash
python benchmarks_windows.py
```

This will run:
- Write throughput tests
- Durability tests (using `taskkill` instead of SIGKILL)
- ACID property tests

### 4. Starting a Server

```bash
python server.py 5000 ./data
```

### 5. Using the Client

```python
from client import KVStoreClient

client = KVStoreClient('localhost', 5000)
client.Set('key', 'value')
value = client.Get('key')
print(value)
```

## Key Differences from Linux Version

### Process Termination
- **Linux**: Uses `SIGTERM` and `SIGKILL` signals
- **Windows**: Uses `taskkill /F /T /PID <pid>` command

### File Locking
- **Linux**: Uses `fcntl.flock()`
- **Windows**: Uses `msvcrt.locking()`

### Process Creation
- **Windows**: Uses `CREATE_NEW_PROCESS_GROUP` flag
- **Linux**: Standard process creation

## Known Issues on Windows

### 1. File Deletion
Sometimes Windows keeps file handles open, preventing immediate deletion of test directories. The tests handle this gracefully with try/except blocks.

### 2. Port Conflicts
If you see "Address already in use" errors, wait a few seconds or change the port numbers in the tests.

### 3. Taskkill Permissions
Some antivirus software may flag `taskkill` commands. This is normal for the tests.

## Running Individual Test Suites

```bash
# Basic tests only
python tests_windows.py TestKVStoreBasic

# Persistence tests
python tests_windows.py TestKVStorePersistence

# Concurrency tests
python tests_windows.py TestKVStoreConcurrency

# Indexing tests
python tests_windows.py TestKVStoreIndexing
```

## Troubleshooting

### "Another instance is already running"
This means a previous server process didn't shut down properly. 

Fix:
```bash
# Find Python processes
tasklist | findstr python

# Kill the process
taskkill /F /PID <pid>
```

### "Connection refused"
The server isn't running or isn't ready yet.

Fix:
- Make sure you started the server
- Wait 2-3 seconds after starting the server
- Check the port number matches

### File Permission Errors
Close any programs that might have the data files open (text editors, file explorers).

## Performance Notes

Windows performance is generally comparable to Linux, but:
- File I/O might be slightly slower
- Process creation has more overhead
- Port binding may take longer

Typical performance on Windows:
- Individual writes: 150-200 writes/sec
- Bulk writes: 1,500-2,000 writes/sec
- Durability: 100% (same as Linux)

## Example Session

```powershell
# Terminal 1 - Start server
PS> python server.py 5000 ./mydata

# Terminal 2 - Use client
PS> python
>>> from client import KVStoreClient
>>> client = KVStoreClient('localhost', 5000)
>>> client.Set('hello', 'world')
True
>>> client.Get('hello')
'world'
>>> client.BulkSet([('a', '1'), ('b', '2'), ('c', '3')])
True
>>> client.SearchText('world')
['hello']
>>> exit()

# Back to Terminal 1
# Press Ctrl+C to stop server
```

## Running Tests in PyCharm

1. Right-click on `tests_windows.py`
2. Select "Run 'Unittests in tests_windows.py'"
3. View results in the test runner panel

## Debugging Tips

### Enable Verbose Output
```python
# In tests_windows.py or benchmarks_windows.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Check Server Logs
The server prints to stdout/stderr. Capture them:
```python
server = subprocess.Popen(
    [...],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)
# Later
print(server.stdout.read())
print(server.stderr.read())
```

### Manual Testing
Start server manually in one terminal, run tests in another:
```bash
# Terminal 1
python server.py 5000 ./test_data

# Terminal 2
python -c "from client import KVStoreClient; c = KVStoreClient(); c.Set('test', 'works'); print(c.Get('test'))"
```

## Next Steps

Once the tests pass:
1. Try the demo (adapt demo.py for Windows)
2. Run the full benchmark suite
3. Build your application using the client library

## Support

If you encounter issues:
1. Check this guide first
2. Verify Python version (3.8+)
3. Make sure no firewall is blocking localhost connections
4. Check the error messages carefully
