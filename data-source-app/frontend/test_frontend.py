#!/usr/bin/env python3
"""
Test script for the metadata frontend.
Demonstrates both command line and web interface usage.
"""

import subprocess
import time
import requests
import json
import sys
import os

def test_command_line_frontend():
    """Test the command line frontend."""
    print("🧪 Testing Command Line Frontend")
    print("=" * 50)
    
    try:
        # Run the command line frontend
        result = subprocess.run([
            sys.executable, 'app.py', 'test-connection', '--config', '../config.yml'
        ], capture_output=True, text=True, cwd='frontend')
        
        if result.returncode == 0:
            print("✅ Command line frontend working!")
            print("Sample output:")
            print(result.stdout[:500] + "..." if len(result.stdout) > 500 else result.stdout)
        else:
            print("❌ Command line frontend failed!")
            print("Error:", result.stderr)
            
    except Exception as e:
        print(f"❌ Error testing command line frontend: {e}")

def test_web_api():
    """Test the web API endpoints."""
    print("\n🌐 Testing Web API")
    print("=" * 50)
    
    base_url = "http://localhost:5001"
    
    try:
        # Test connections endpoint
        print("Testing /api/connections...")
        response = requests.get(f"{base_url}/api/connections", timeout=5)
        if response.status_code == 200:
            connections = response.json()
            print(f"✅ Found {len(connections)} connections")
            for conn in connections:
                print(f"  - {conn['connection_id']} ({conn['connector_name']})")
        else:
            print(f"❌ Connections API failed: {response.status_code}")
            return
        
        # Test metadata endpoint
        print("\nTesting /api/metadata/test-connection...")
        response = requests.get(f"{base_url}/api/metadata/test-connection", timeout=5)
        if response.status_code == 200:
            metadata = response.json()
            print("✅ Metadata API working!")
            print(f"  - Sync ID: {metadata['sync_run']['sync_id'][:8]}...")
            print(f"  - Schemas: {len(metadata['metadata']['schemas'])}")
            print(f"  - Tables: {len(metadata['metadata']['tables'])}")
            print(f"  - Columns: {len(metadata['metadata']['columns'])}")
        else:
            print(f"❌ Metadata API failed: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Web server not running. Start it with: python frontend/web_app.py")
    except Exception as e:
        print(f"❌ Error testing web API: {e}")

def test_web_interface():
    """Test the web interface."""
    print("\n🖥️  Testing Web Interface")
    print("=" * 50)
    
    base_url = "http://localhost:5001"
    
    try:
        # Test main page
        print("Testing main page...")
        response = requests.get(base_url, timeout=5)
        if response.status_code == 200:
            print("✅ Main page accessible")
            if "Available Connections" in response.text:
                print("✅ Connections list displayed")
            else:
                print("⚠️  Connections list not found")
        else:
            print(f"❌ Main page failed: {response.status_code}")
            return
        
        # Test metadata page
        print("\nTesting metadata page...")
        response = requests.get(f"{base_url}/metadata/test-connection", timeout=5)
        if response.status_code == 200:
            print("✅ Metadata page accessible")
            if "test-connection" in response.text:
                print("✅ Connection metadata displayed")
            else:
                print("⚠️  Connection metadata not found")
        else:
            print(f"❌ Metadata page failed: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Web server not running. Start it with: python frontend/web_app.py")
    except Exception as e:
        print(f"❌ Error testing web interface: {e}")

def main():
    """Run all frontend tests."""
    print("🚀 Frontend Test Suite")
    print("=" * 60)
    
    # Test command line frontend
    test_command_line_frontend()
    
    # Test web API
    test_web_api()
    
    # Test web interface
    test_web_interface()
    
    print("\n" + "=" * 60)
    print("✅ Frontend testing completed!")
    print("\nTo use the frontend:")
    print("1. Command line: python frontend/app.py <connection_id> --config config.yml")
    print("2. Web interface: python frontend/web_app.py (then visit http://localhost:5001)")
    print("3. API: curl http://localhost:5001/api/connections")

if __name__ == "__main__":
    main()
