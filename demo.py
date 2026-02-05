#!/usr/bin/env python3
"""
Demo script showcasing all features of the distributed KV store.
"""
import subprocess
import time
import signal
import sys
from client import KVStoreClient


def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def demo_basic_operations():
    """Demo basic CRUD operations."""
    print_section("BASIC OPERATIONS")
    
    client = KVStoreClient('localhost', 5000)
    
    print("\n1. Setting key-value pairs...")
    client.Set('name', 'Alice')
    client.Set('age', '30')
    client.Set('city', 'San Francisco')
    print("   ✓ Set 3 keys")
    
    print("\n2. Getting values...")
    name = client.Get('name')
    age = client.Get('age')
    city = client.Get('city')
    print(f"   name = {name}")
    print(f"   age = {age}")
    print(f"   city = {city}")
    
    print("\n3. Updating a value...")
    client.Set('age', '31')
    new_age = client.Get('age')
    print(f"   Updated age = {new_age}")
    
    print("\n4. Deleting a key...")
    client.Delete('city')
    deleted_value = client.Get('city')
    print(f"   city after delete = {deleted_value} (None expected)")
    
    print("\n5. Bulk set...")
    items = [
        ('user1', 'Alice'),
        ('user2', 'Bob'),
        ('user3', 'Charlie'),
        ('user4', 'David'),
        ('user5', 'Eve')
    ]
    client.BulkSet(items)
    print(f"   ✓ Bulk set {len(items)} items")
    
    # Verify
    print("\n6. Verifying bulk set...")
    for key, expected_value in items:
        value = client.Get(key)
        print(f"   {key} = {value} {'✓' if value == expected_value else '✗'}")


def demo_persistence():
    """Demo persistence across restarts."""
    print_section("PERSISTENCE ACROSS RESTARTS")
    
    print("\n1. Starting server...")
    server = subprocess.Popen(
        ['python3', 'server.py', '5001', './demo_persist_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    
    print("2. Writing data...")
    client = KVStoreClient('localhost', 5001)
    client.Set('persistent_key', 'This value should survive restart')
    client.BulkSet([
        ('data1', 'value1'),
        ('data2', 'value2'),
        ('data3', 'value3')
    ])
    print("   ✓ Data written")
    
    print("\n3. Stopping server gracefully...")
    server.send_signal(signal.SIGTERM)
    server.wait()
    time.sleep(1)
    
    print("4. Restarting server...")
    server = subprocess.Popen(
        ['python3', 'server.py', '5001', './demo_persist_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    
    print("5. Retrieving data...")
    client = KVStoreClient('localhost', 5001)
    value = client.Get('persistent_key')
    data1 = client.Get('data1')
    data2 = client.Get('data2')
    data3 = client.Get('data3')
    
    print(f"   persistent_key = {value}")
    print(f"   data1 = {data1}")
    print(f"   data2 = {data2}")
    print(f"   data3 = {data3}")
    
    if value and data1 and data2 and data3:
        print("\n   ✓ ALL DATA SURVIVED RESTART!")
    else:
        print("\n   ✗ Some data was lost")
    
    # Cleanup
    server.send_signal(signal.SIGTERM)
    server.wait()


def demo_full_text_search():
    """Demo full-text search using inverted index."""
    print_section("FULL-TEXT SEARCH")
    
    client = KVStoreClient('localhost', 5000)
    
    print("\n1. Adding documents...")
    docs = {
        'doc1': 'the quick brown fox jumps over the lazy dog',
        'doc2': 'machine learning and artificial intelligence',
        'doc3': 'deep learning neural networks',
        'doc4': 'python programming language',
        'doc5': 'database systems and indexing',
        'doc6': 'distributed systems and replication'
    }
    
    for key, value in docs.items():
        client.Set(key, value)
    print(f"   ✓ Added {len(docs)} documents")
    
    print("\n2. Searching for documents...")
    
    queries = [
        ('learning', ['doc2', 'doc3']),
        ('python', ['doc4']),
        ('systems', ['doc5', 'doc6']),
        ('quick lazy', ['doc1'])
    ]
    
    for query, expected_keys in queries:
        results = client.SearchText(query)
        print(f"\n   Query: '{query}'")
        print(f"   Results: {results}")
        print(f"   Expected: {expected_keys}")
        
        if set(results) == set(expected_keys):
            print("   ✓ Correct!")
        else:
            print("   ✗ Mismatch")


def demo_similarity_search():
    """Demo similarity search using embeddings."""
    print_section("SIMILARITY SEARCH")
    
    client = KVStoreClient('localhost', 5000)
    
    print("\n1. Adding documents with different topics...")
    docs = {
        'ai1': 'machine learning artificial intelligence deep learning',
        'ai2': 'neural networks backpropagation training',
        'ai3': 'computer vision image recognition',
        'prog1': 'python java programming languages',
        'prog2': 'software development coding',
        'food1': 'cooking recipes delicious meals',
        'food2': 'restaurants dining experience'
    }
    
    for key, value in docs.items():
        client.Set(key, value)
    print(f"   ✓ Added {len(docs)} documents")
    
    print("\n2. Finding similar documents...")
    
    queries = [
        'AI and neural nets',
        'programming in python',
        'good food'
    ]
    
    for query in queries:
        results = client.SearchSimilar(query, top_k=3)
        print(f"\n   Query: '{query}'")
        print("   Top 3 most similar:")
        for i, (key, score) in enumerate(results[:3], 1):
            print(f"     {i}. {key} (similarity: {score:.3f})")


def demo_crash_recovery():
    """Demo crash recovery with WAL."""
    print_section("CRASH RECOVERY")
    
    print("\n1. Starting server...")
    server = subprocess.Popen(
        ['python3', 'server.py', '5002', './demo_crash_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    
    print("2. Writing data...")
    client = KVStoreClient('localhost', 5002)
    for i in range(20):
        client.Set(f'crash_key_{i}', f'crash_value_{i}')
    print("   ✓ Wrote 20 keys")
    
    print("\n3. KILLING server with SIGKILL (simulating crash)...")
    server.send_signal(signal.SIGKILL)
    server.wait()
    time.sleep(1)
    
    print("4. Restarting server (should recover from WAL)...")
    server = subprocess.Popen(
        ['python3', 'server.py', '5002', './demo_crash_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    
    print("5. Verifying data recovery...")
    client = KVStoreClient('localhost', 5002)
    
    recovered = 0
    lost = 0
    for i in range(20):
        value = client.Get(f'crash_key_{i}')
        if value == f'crash_value_{i}':
            recovered += 1
        else:
            lost += 1
    
    print(f"   Recovered: {recovered}/20")
    print(f"   Lost: {lost}/20")
    
    if recovered == 20:
        print("\n   ✓ 100% RECOVERY - ALL DATA SURVIVED CRASH!")
    else:
        print(f"\n   ✗ Lost {lost} keys")
    
    # Cleanup
    server.send_signal(signal.SIGTERM)
    server.wait()


def main():
    """Run all demos."""
    print("\n" + "=" * 60)
    print("  DISTRIBUTED KEY-VALUE STORE - FEATURE DEMO")
    print("=" * 60)
    
    # Start main server
    print("\nStarting main server on port 5000...")
    server = subprocess.Popen(
        ['python3', 'server.py', '5000', './demo_data'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    print("✓ Server started")
    
    try:
        # Run demos
        demo_basic_operations()
        demo_full_text_search()
        demo_similarity_search()
        demo_persistence()
        demo_crash_recovery()
        
        print("\n" + "=" * 60)
        print("  DEMO COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during demo: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        print("\nCleaning up...")
        server.send_signal(signal.SIGTERM)
        server.wait()
        
        # Clean up demo data directories
        import shutil
        import os
        for dir_name in ['demo_data', 'demo_persist_data', 'demo_crash_data']:
            if os.path.exists(dir_name):
                shutil.rmtree(dir_name)
        
        print("✓ Cleanup complete")


if __name__ == '__main__':
    main()
