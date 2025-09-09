#!/usr/bin/env python3
"""
Deployment script for the Event Hub Delta Pipeline.
"""

import os
import sys
import subprocess
import json
from pathlib import Path


def run_command(command, check=True):
    """Run a shell command and return the result."""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if check and result.returncode != 0:
        print(f"Error running command: {command}")
        print(f"Error output: {result.stderr}")
        sys.exit(1)
    
    return result


def deploy_infrastructure():
    """Deploy Azure infrastructure using ARM template."""
    print("Deploying Azure infrastructure...")
    
    result = run_command("az --version", check=False)
    if result.returncode != 0:
        print("Azure CLI is not installed. Please install it first.")
        sys.exit(1)
    
    result = run_command("az account show", check=False)
    if result.returncode != 0:
        print("Please log in to Azure CLI first: az login")
        sys.exit(1)
    
    resource_group = input("Enter resource group name (or press Enter to create new): ").strip()
    if not resource_group:
        resource_group = "eventhub-delta-pipeline-rg"
        print(f"Creating new resource group: {resource_group}")
        run_command(f"az group create --name {resource_group} --location eastus")
    
    template_path = "infrastructure/arm-template.json"
    deployment_name = "eventhub-delta-deployment"
    
    print(f"Deploying ARM template to resource group: {resource_group}")
    result = run_command(
        f"az deployment group create "
        f"--resource-group {resource_group} "
        f"--template-file {template_path} "
        f"--name {deployment_name}"
    )
    
    outputs_result = run_command(
        f"az deployment group show "
        f"--resource-group {resource_group} "
        f"--name {deployment_name} "
        f"--query properties.outputs"
    )
    
    outputs = json.loads(outputs_result.stdout)
    print("\nDeployment completed successfully!")
    print("Resource details:")
    for key, value in outputs.items():
        print(f"  {key}: {value.get('value', 'N/A')}")
    
    return outputs


def setup_local_environment():
    """Set up local development environment."""
    print("Setting up local development environment...")
    
    print("Installing Python dependencies...")
    run_command("pip install -r requirements.txt")
    
    env_file = Path(".env")
    if not env_file.exists():
        print("Creating .env file from template...")
        env_example = Path(".env.example")
        if env_example.exists():
            with open(env_example) as f:
                content = f.read()
            with open(env_file, 'w') as f:
                f.write(content)
            print("Please update .env file with your Azure credentials and connection strings.")
        else:
            print("Warning: .env.example not found")
    
    print("Local environment setup completed!")


def run_tests():
    """Run unit tests."""
    print("Running unit tests...")
    
    run_command("pip install pytest pytest-cov")
    
    result = run_command("python -m pytest tests/ -v", check=False)
    if result.returncode != 0:
        print("Some tests failed. Please check the output above.")
        return False
    
    print("All tests passed!")
    return True


def main():
    """Main deployment function."""
    print("Event Hub Delta Pipeline Deployment Script")
    print("=" * 50)
    
    if len(sys.argv) > 1:
        action = sys.argv[1]
    else:
        print("Available actions:")
        print("  infrastructure - Deploy Azure infrastructure")
        print("  local - Set up local development environment")
        print("  test - Run unit tests")
        print("  all - Run all deployment steps")
        action = input("Select action: ").strip()
    
    try:
        if action == "infrastructure":
            deploy_infrastructure()
        elif action == "local":
            setup_local_environment()
        elif action == "test":
            run_tests()
        elif action == "all":
            setup_local_environment()
            if run_tests():
                print("\nLocal setup and tests completed successfully!")
                print("To deploy infrastructure, run: python deploy.py infrastructure")
        else:
            print(f"Unknown action: {action}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nDeployment cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
