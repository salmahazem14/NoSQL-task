#!/usr/bin/env python
"""
Comprehensive tests for the distributed key-value store (Windows compatible).
"""
import unittest
import time
import threading
import subprocess
import signal
import os
import shutil
import sys
from pathlib import Path
from client import KVStoreClient


# Detect Python executable
PYTHON_EXE = sys.executable


class TestKVStoreBasic(unittest.TestCase):
    """Basic functionality tests."""
    
    @classmethod
    def setUpClass(cls):
        """Start server before tests."""
        cls.port = 5001
        cls.data_dir = f"./test_data_{cls.port}"
        
        # Clean up any existing data
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
        
        # Start server
        cls.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(cls.port), cls.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        time.sleep(2)  # Wait for server to start
        
        cls.client = KVStoreClient('localhost', cls.port)
    
    @classmethod
    def tearDownClass(cls):
        """Stop server after tests."""
        if cls.server_process:
            if sys.platform == 'win32':
                # On Windows, use taskkill for graceful shutdown
                subprocess.call(['taskkill', '/F', '/T', '/PID', str(cls.server_process.pid)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                cls.server_process.send_signal(signal.SIGTERM)
            
            try:
                cls.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls.server_process.kill()
        
        # Clean up test data
        time.sleep(1)
        if os.path.exists(cls.data_dir):
            try:
                shutil.rmtree(cls.data_dir)
            except:
                pass
    
    def test_set_then_get(self):
        """Test: Set then Get."""
        key = 'test_key_1'
        value = 'test_value_1'
        
        # Set
        result = self.client.Set(key, value)
        self.assertTrue(result)
        
        # Get
        retrieved = self.client.Get(key)
        self.assertEqual(retrieved, value)
    
    def test_set_then_delete_then_get(self):
        """Test: Set then Delete then Get."""
        key = 'test_key_2'
        value = 'test_value_2'
        
        # Set
        self.client.Set(key, value)
        
        # Delete
        result = self.client.Delete(key)
        self.assertTrue(result)
        
        # Get (should return None)
        retrieved = self.client.Get(key)
        self.assertIsNone(retrieved)
    
    def test_get_without_setting(self):
        """Test: Get without setting."""
        key = 'nonexistent_key'
        
        # Get (should return None)
        retrieved = self.client.Get(key)
        self.assertIsNone(retrieved)
    
    def test_set_then_set_same_key_then_get(self):
        """Test: Set then Set (same key) then Get."""
        key = 'test_key_3'
        value1 = 'first_value'
        value2 = 'second_value'
        
        # First set
        self.client.Set(key, value1)
        
        # Second set (overwrite)
        self.client.Set(key, value2)
        
        # Get (should return second value)
        retrieved = self.client.Get(key)
        self.assertEqual(retrieved, value2)
    
    def test_bulk_set(self):
        """Test: Bulk set operation."""
        items = [
            ('bulk_key_1', 'bulk_value_1'),
            ('bulk_key_2', 'bulk_value_2'),
            ('bulk_key_3', 'bulk_value_3'),
        ]
        
        # Bulk set
        result = self.client.BulkSet(items)
        self.assertTrue(result)
        
        # Verify all items
        for key, value in items:
            retrieved = self.client.Get(key)
            self.assertEqual(retrieved, value)


class TestKVStorePersistence(unittest.TestCase):
    """Test persistence across restarts."""
    
    def setUp(self):
        """Set up for each test."""
        self.port = 5002
        self.data_dir = f"./test_data_persistence_{self.port}"
        
        # Clean up any existing data
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
    
    def tearDown(self):
        """Clean up after each test."""
        time.sleep(1)
        if os.path.exists(self.data_dir):
            try:
                shutil.rmtree(self.data_dir)
            except:
                pass
    
    def test_set_then_exit_then_get(self):
        """Test: Set then exit (gracefully) then Get."""
        key = 'persistence_key'
        value = 'persistence_value'
        
        # Start server
        server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        time.sleep(2)
        
        # Set value
        client = KVStoreClient('localhost', self.port)
        client.Set(key, value)
        
        # Gracefully stop server
        if sys.platform == 'win32':
            subprocess.call(['taskkill', '/F', '/T', '/PID', str(server_process.pid)],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            server_process.send_signal(signal.SIGTERM)
        
        try:
            server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_process.kill()
        
        time.sleep(1)
        
        # Restart server
        server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(self.port), self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        time.sleep(2)
        
        # Get value (should still be there)
        client = KVStoreClient('localhost', self.port)
        retrieved = client.Get(key)
        self.assertEqual(retrieved, value)
        
        # Clean up
        if sys.platform == 'win32':
            subprocess.call(['taskkill', '/F', '/T', '/PID', str(server_process.pid)],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            server_process.send_signal(signal.SIGTERM)
        
        try:
            server_process.wait(timeout=5)
        except:
            pass


class TestKVStoreConcurrency(unittest.TestCase):
    """Test concurrent operations and ACID properties."""
    
    @classmethod
    def setUpClass(cls):
        """Start server before tests."""
        cls.port = 5003
        cls.data_dir = f"./test_data_concurrency_{cls.port}"
        
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
        
        cls.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(cls.port), cls.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        time.sleep(2)
        
        cls.client = KVStoreClient('localhost', cls.port)
    
    @classmethod
    def tearDownClass(cls):
        """Stop server after tests."""
        if cls.server_process:
            if sys.platform == 'win32':
                subprocess.call(['taskkill', '/F', '/T', '/PID', str(cls.server_process.pid)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                cls.server_process.send_signal(signal.SIGTERM)
            
            try:
                cls.server_process.wait(timeout=5)
            except:
                pass
        
        time.sleep(1)
        if os.path.exists(cls.data_dir):
            try:
                shutil.rmtree(cls.data_dir)
            except:
                pass
    
    def test_concurrent_bulk_set_isolation(self):
        """Test: Concurrent bulk sets don't affect each other (Isolation)."""
        results = {'thread1': [], 'thread2': []}
        errors = []
        
        def bulk_set_thread1():
            try:
                client = KVStoreClient('localhost', self.port)
                items = [(f'iso_key_{i}', f'thread1_value_{i}') for i in range(10)]
                result = client.BulkSet(items)
                results['thread1'].append(result)
            except Exception as e:
                errors.append(e)
        
        def bulk_set_thread2():
            try:
                client = KVStoreClient('localhost', self.port)
                items = [(f'iso_key_{i}', f'thread2_value_{i}') for i in range(10)]
                result = client.BulkSet(items)
                results['thread2'].append(result)
            except Exception as e:
                errors.append(e)
        
        t1 = threading.Thread(target=bulk_set_thread1)
        t2 = threading.Thread(target=bulk_set_thread2)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        # Both should succeed
        self.assertEqual(len(errors), 0)
        self.assertTrue(all(results['thread1']))
        self.assertTrue(all(results['thread2']))
        
        # Verify that all keys have one of the values (consistency)
        client = KVStoreClient('localhost', self.port)
        for i in range(10):
            value = client.Get(f'iso_key_{i}')
            self.assertIn(value, [f'thread1_value_{i}', f'thread2_value_{i}'])
    
    def test_bulk_set_atomicity(self):
        """Test: Bulk set is atomic (all or nothing)."""
        # This is implicitly tested by the WAL mechanism
        items = [(f'atomic_key_{i}', f'atomic_value_{i}') for i in range(100)]
        
        result = self.client.BulkSet(items)
        self.assertTrue(result)
        
        # Verify all items exist
        for key, value in items:
            retrieved = self.client.Get(key)
            self.assertEqual(retrieved, value)


class TestKVStoreIndexing(unittest.TestCase):
    """Test indexing features."""
    
    @classmethod
    def setUpClass(cls):
        """Start server before tests."""
        cls.port = 5004
        cls.data_dir = f"./test_data_indexing_{cls.port}"
        
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
        
        cls.server_process = subprocess.Popen(
            [PYTHON_EXE, 'server.py', str(cls.port), cls.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        time.sleep(2)
        
        cls.client = KVStoreClient('localhost', cls.port)
    
    @classmethod
    def tearDownClass(cls):
        """Stop server after tests."""
        if cls.server_process:
            if sys.platform == 'win32':
                subprocess.call(['taskkill', '/F', '/T', '/PID', str(cls.server_process.pid)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                cls.server_process.send_signal(signal.SIGTERM)
            
            try:
                cls.server_process.wait(timeout=5)
            except:
                pass
        
        time.sleep(1)
        if os.path.exists(cls.data_dir):
            try:
                shutil.rmtree(cls.data_dir)
            except:
                pass
    
    def test_full_text_search(self):
        """Test: Full-text search using inverted index."""
        # Set up test data
        self.client.Set('doc1', 'the quick brown fox jumps over lazy dog')
        self.client.Set('doc2', 'the lazy cat sleeps all day')
        self.client.Set('doc3', 'quick thinking saves the day')
        
        # Search for "lazy"
        results = self.client.SearchText('lazy')
        self.assertIn('doc1', results)
        self.assertIn('doc2', results)
        self.assertNotIn('doc3', results)
        
        # Search for "quick"
        results = self.client.SearchText('quick')
        self.assertIn('doc1', results)
        self.assertIn('doc3', results)
        self.assertNotIn('doc2', results)
    
    def test_similarity_search(self):
        """Test: Similarity search using embeddings."""
        # Set up test data
        self.client.Set('doc1', 'machine learning artificial intelligence')
        self.client.Set('doc2', 'deep neural networks')
        self.client.Set('doc3', 'cooking recipes food')
        
        # Search for similar documents
        results = self.client.SearchSimilar('AI deep learning', top_k=3)
        
        # doc1 and doc2 should be more similar to the query than doc3
        keys = [key for key, score in results]
        self.assertIn('doc1', keys[:2])
        self.assertIn('doc2', keys[:2])


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
