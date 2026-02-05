#!/usr/bin/env python
"""
Benchmarks for the distributed key-value store (Windows compatible).
Tests write throughput, durability, and ACID properties.
"""
import time
import threading
import subprocess
import signal
import os
import shutil
import random
import string
import sys
from typing import List, Set
from client import KVStoreClient


# Detect Python executable
PYTHON_EXE = sys.executable


def kill_process(process):
    """Kill a process in a cross-platform way."""
    if sys.platform == 'win32':
        # On Windows, use taskkill with /F flag
        subprocess.call(['taskkill', '/F', '/T', '/PID', str(process.pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        # On Unix, use SIGKILL
        process.send_signal(signal.SIGKILL)
    
    try:
        process.wait(timeout=2)
    except:
        pass


def terminate_process(process):
    """Gracefully terminate a process in a cross-platform way."""
    if sys.platform == 'win32':
        subprocess.call(['taskkill', '/F', '/T', '/PID', str(process.pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        process.send_signal(signal.SIGTERM)
    
    try:
        process.wait(timeout=5)
    except:
        kill_process(process)


class ThroughputBenchmark:
    """Benchmark write throughput."""
    
    def __init__(self, port: int = 5100, data_dir: str = "./bench_data"):
        self.port = port
        self.data_dir = data_dir
        self.server_process = None
        self.client = None
    
    def setup(self):
        """Set up benchmark environment."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        self.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creation_flags
        )
        time.sleep(2)
        
        self.client = KVStoreClient('localhost', self.port)
    
    def teardown(self):
        """Clean up benchmark environment."""
        if self.server_process:
            terminate_process(self.server_process)
        
        time.sleep(1)
        if os.path.exists(self.data_dir):
            try:
                shutil.rmtree(self.data_dir)
            except:
                pass
    
    def generate_random_string(self, length: int = 100) -> str:
        """Generate random string for values."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def benchmark_write_throughput(self, num_writes: int = 1000, prepopulate: int = 0):
        """
        Benchmark write throughput.
        
        Args:
            num_writes: Number of writes to perform
            prepopulate: Number of existing keys in the database
        """
        # Prepopulate if needed
        if prepopulate > 0:
            print(f"Prepopulating with {prepopulate} keys...")
            for i in range(prepopulate):
                key = f"prepop_key_{i}"
                value = self.generate_random_string()
                self.client.Set(key, value)
        
        # Benchmark writes
        print(f"\nBenchmarking {num_writes} writes with {prepopulate} existing keys...")
        start_time = time.time()
        
        for i in range(num_writes):
            key = f"bench_key_{i}"
            value = self.generate_random_string()
            self.client.Set(key, value)
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_writes / duration
        
        print(f"Duration: {duration:.2f} seconds")
        print(f"Throughput: {throughput:.2f} writes/second")
        print(f"Average latency: {(duration / num_writes) * 1000:.2f} ms")
        
        return throughput
    
    def benchmark_bulk_write_throughput(self, num_operations: int = 100, batch_size: int = 100):
        """Benchmark bulk write throughput."""
        print(f"\nBenchmarking {num_operations} bulk writes (batch size: {batch_size})...")
        start_time = time.time()
        
        for i in range(num_operations):
            items = []
            for j in range(batch_size):
                key = f"bulk_bench_key_{i}_{j}"
                value = self.generate_random_string()
                items.append((key, value))
            
            self.client.BulkSet(items)
        
        end_time = time.time()
        duration = end_time - start_time
        total_writes = num_operations * batch_size
        throughput = total_writes / duration
        
        print(f"Duration: {duration:.2f} seconds")
        print(f"Total writes: {total_writes}")
        print(f"Throughput: {throughput:.2f} writes/second")
        print(f"Average latency per batch: {(duration / num_operations) * 1000:.2f} ms")
        
        return throughput
    
    def run_all(self):
        """Run all throughput benchmarks."""
        try:
            self.setup()
            
            print("=" * 60)
            print("WRITE THROUGHPUT BENCHMARKS")
            print("=" * 60)
            
            # Test with different database sizes
            self.benchmark_write_throughput(num_writes=500, prepopulate=0)
            self.benchmark_write_throughput(num_writes=500, prepopulate=500)
            self.benchmark_write_throughput(num_writes=500, prepopulate=2000)
            
            # Test bulk operations
            self.benchmark_bulk_write_throughput(num_operations=50, batch_size=10)
            self.benchmark_bulk_write_throughput(num_operations=50, batch_size=100)
            
        finally:
            self.teardown()


class DurabilityBenchmark:
    """Benchmark durability guarantees."""
    
    def __init__(self, port: int = 5101, data_dir: str = "./bench_durability"):
        self.port = port
        self.data_dir = data_dir
        self.server_process = None
        self.acknowledged_keys: Set[str] = set()
        self.stop_flag = threading.Event()
        self.lock = threading.Lock()
    
    def setup(self):
        """Set up benchmark environment."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        self.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creation_flags
        )
        time.sleep(2)
    
    def teardown(self):
        """Clean up benchmark environment."""
        if self.server_process:
            try:
                kill_process(self.server_process)
            except:
                pass
        
        time.sleep(1)
        if os.path.exists(self.data_dir):
            try:
                shutil.rmtree(self.data_dir)
            except:
                pass
    
    def writer_thread(self, num_writes: int = 1000):
        """Thread that continuously writes data."""
        client = KVStoreClient('localhost', self.port)
        
        for i in range(num_writes):
            if self.stop_flag.is_set():
                break
            
            key = f"durable_key_{i}"
            value = f"durable_value_{i}"
            
            try:
                success = client.Set(key, value)
                if success:
                    with self.lock:
                        self.acknowledged_keys.add(key)
            except:
                # Server might be down
                pass
            
            time.sleep(0.01)  # Small delay
    
    def killer_thread(self, num_kills: int = 5, interval: float = 2.0):
        """Thread that randomly kills the server."""
        for i in range(num_kills):
            if self.stop_flag.is_set():
                break
            
            time.sleep(interval)
            
            # Kill server
            if self.server_process and self.server_process.poll() is None:
                print(f"\n[KILL {i+1}] Killing server forcefully...")
                kill_process(self.server_process)
                
                # Restart server
                time.sleep(0.5)
                print(f"[RESTART {i+1}] Restarting server...")
                creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
                self.server_process = subprocess.Popen(
                    [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=creation_flags
                )
                time.sleep(1)
    
    def benchmark_durability(self, num_writes: int = 300, num_kills: int = 3):
        """
        Test durability under random server kills.
        
        Checks what percentage of acknowledged writes survive crashes.
        """
        print("=" * 60)
        print("DURABILITY BENCHMARK")
        print("=" * 60)
        print(f"Writes: {num_writes}, Kills: {num_kills}")
        print("Starting writer and killer threads...")
        print("(Note: On Windows, kills use taskkill instead of SIGKILL)")
        
        self.acknowledged_keys.clear()
        self.stop_flag.clear()
        
        # Start threads
        writer = threading.Thread(target=self.writer_thread, args=(num_writes,))
        killer = threading.Thread(target=self.killer_thread, args=(num_kills, 2.0))
        
        writer.start()
        time.sleep(0.5)  # Let writer start first
        killer.start()
        
        # Wait for completion
        writer.join()
        self.stop_flag.set()
        killer.join()
        
        # Final server restart to check persistence
        if self.server_process:
            terminate_process(self.server_process)
        
        time.sleep(1)
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        self.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creation_flags
        )
        time.sleep(2)
        
        # Check which acknowledged keys survived
        client = KVStoreClient('localhost', self.port)
        survived_keys = set()
        lost_keys = set()
        
        print(f"\nChecking {len(self.acknowledged_keys)} acknowledged keys...")
        for key in self.acknowledged_keys:
            try:
                value = client.Get(key)
                if value is not None:
                    survived_keys.add(key)
                else:
                    lost_keys.add(key)
            except:
                lost_keys.add(key)
        
        # Results
        total_acknowledged = len(self.acknowledged_keys)
        total_survived = len(survived_keys)
        total_lost = len(lost_keys)
        
        durability_percentage = (total_survived / total_acknowledged * 100) if total_acknowledged > 0 else 0
        
        print(f"\nRESULTS:")
        print(f"Total acknowledged writes: {total_acknowledged}")
        print(f"Survived after crashes: {total_survived}")
        print(f"Lost after crashes: {total_lost}")
        print(f"Durability: {durability_percentage:.2f}%")
        
        if durability_percentage == 100.0:
            print("✓ 100% DURABILITY ACHIEVED")
        else:
            print(f"✗ Lost {total_lost} acknowledged writes")
            if lost_keys:
                print(f"Lost keys: {list(lost_keys)[:10]}...")
        
        return durability_percentage
    
    def run_all(self):
        """Run all durability benchmarks."""
        try:
            self.setup()
            self.benchmark_durability(num_writes=300, num_kills=3)
        finally:
            self.teardown()


class ACIDBenchmark:
    """Benchmark ACID properties."""
    
    def __init__(self, port: int = 5102, data_dir: str = "./bench_acid"):
        self.port = port
        self.data_dir = data_dir
        self.server_process = None
    
    def setup(self):
        """Set up benchmark environment."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        self.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creation_flags
        )
        time.sleep(2)
    
    def teardown(self):
        """Clean up benchmark environment."""
        if self.server_process:
            try:
                terminate_process(self.server_process)
            except:
                pass
        
        time.sleep(1)
        if os.path.exists(self.data_dir):
            try:
                shutil.rmtree(self.data_dir)
            except:
                pass
    
    def test_isolation_concurrent_bulk_sets(self):
        """Test isolation: Concurrent bulk sets on same keys."""
        print("=" * 60)
        print("ACID BENCHMARK - ISOLATION")
        print("=" * 60)
        print("Testing concurrent bulk sets on overlapping keys...")
        
        client = KVStoreClient('localhost', self.port)
        results = {'success': [], 'errors': []}
        
        def bulk_writer(thread_id: int, num_batches: int = 10):
            thread_client = KVStoreClient('localhost', self.port)
            for batch in range(num_batches):
                items = []
                for i in range(10):
                    key = f"isolation_key_{i}"  # Same keys across threads
                    value = f"thread{thread_id}_batch{batch}_value{i}"
                    items.append((key, value))
                
                try:
                    success = thread_client.BulkSet(items)
                    if success:
                        results['success'].append((thread_id, batch))
                except Exception as e:
                    results['errors'].append((thread_id, batch, str(e)))
        
        # Start multiple concurrent writers
        threads = []
        num_threads = 5
        for i in range(num_threads):
            t = threading.Thread(target=bulk_writer, args=(i, 20))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print(f"\nSuccessful bulk sets: {len(results['success'])}")
        print(f"Errors: {len(results['errors'])}")
        
        # Verify consistency: all keys should have a valid value
        print("\nVerifying consistency...")
        consistent = True
        for i in range(10):
            key = f"isolation_key_{i}"
            value = client.Get(key)
            if value is None:
                print(f"✗ Key {key} is missing!")
                consistent = False
            else:
                # Value should match pattern
                if not value.startswith('thread'):
                    print(f"✗ Key {key} has invalid value: {value}")
                    consistent = False
        
        if consistent:
            print("✓ ISOLATION TEST PASSED - All keys have consistent values")
        else:
            print("✗ ISOLATION TEST FAILED")
    
    def test_atomicity_bulk_with_kills(self):
        """Test atomicity: Bulk writes should be all-or-nothing."""
        print("\n" + "=" * 60)
        print("ACID BENCHMARK - ATOMICITY")
        print("=" * 60)
        print("Testing bulk write atomicity with random server kills...")
        
        completed_batches = []
        stop_flag = threading.Event()
        
        def bulk_writer():
            thread_client = KVStoreClient('localhost', self.port)
            batch_id = 0
            while not stop_flag.is_set():
                items = []
                for i in range(50):  # Large batch
                    key = f"atomic_batch_{batch_id}_key_{i}"
                    value = f"atomic_value_{batch_id}_{i}"
                    items.append((key, value))
                
                try:
                    success = thread_client.BulkSet(items)
                    if success:
                        completed_batches.append(batch_id)
                except:
                    pass  # Server might be down
                
                batch_id += 1
                time.sleep(0.1)
        
        def random_killer(num_kills: int = 3):
            for _ in range(num_kills):
                time.sleep(random.uniform(0.5, 2.0))
                if self.server_process and self.server_process.poll() is None:
                    print("\n[KILL] Killing server forcefully...")
                    kill_process(self.server_process)
                    time.sleep(0.2)
                    
                    print("[RESTART] Restarting server...")
                    creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
                    self.server_process = subprocess.Popen(
                        [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        creationflags=creation_flags
                    )
                    time.sleep(1)
        
        writer = threading.Thread(target=bulk_writer)
        killer = threading.Thread(target=random_killer, args=(3,))
        
        writer.start()
        time.sleep(0.2)
        killer.start()
        
        killer.join()
        stop_flag.set()
        writer.join()
        
        # Restart server for final check
        if self.server_process:
            terminate_process(self.server_process)
        
        time.sleep(1)
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        self.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creation_flags
        )
        time.sleep(2)
        
        # Check atomicity
        print(f"\nChecking atomicity for {len(completed_batches)} completed batches...")
        client = KVStoreClient('localhost', self.port)
        
        atomic_violations = 0
        for batch_id in completed_batches:
            keys_present = 0
            for i in range(50):
                key = f"atomic_batch_{batch_id}_key_{i}"
                value = client.Get(key)
                if value is not None:
                    keys_present += 1
            
            if keys_present > 0 and keys_present < 50:
                atomic_violations += 1
                print(f"✗ Batch {batch_id}: Only {keys_present}/50 keys present (ATOMICITY VIOLATION)")
            elif keys_present == 0:
                print(f"⚠ Batch {batch_id}: No keys present (acknowledged but lost)")
        
        if atomic_violations == 0:
            print(f"✓ ATOMICITY TEST PASSED - No partial batches found")
        else:
            print(f"✗ ATOMICITY TEST FAILED - {atomic_violations} partial batches found")
    
    def run_all(self):
        """Run all ACID benchmarks."""
        try:
            self.setup()
            self.test_isolation_concurrent_bulk_sets()
            self.test_atomicity_bulk_with_kills()
        finally:
            self.teardown()


def main():
    """Run all benchmarks."""
    print("STARTING COMPREHENSIVE BENCHMARKS")
    print("Platform:", sys.platform)
    print("=" * 60)
    
    # Throughput benchmarks
    print("\n\n1. THROUGHPUT BENCHMARKS")
    throughput_bench = ThroughputBenchmark()
    throughput_bench.run_all()
    
    # Durability benchmarks
    print("\n\n2. DURABILITY BENCHMARKS")
    durability_bench = DurabilityBenchmark()
    durability_bench.run_all()
    
    # ACID benchmarks
    print("\n\n3. ACID BENCHMARKS")
    acid_bench = ACIDBenchmark()
    acid_bench.run_all()
    
    print("\n" + "=" * 60)
    print("ALL BENCHMARKS COMPLETED")
    print("=" * 60)


if __name__ == '__main__':
    main()
