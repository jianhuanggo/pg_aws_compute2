"""
Example script demonstrating how to use the key pair creation functionality.

This script shows:
1. How to create a username.json file
2. How to run the key pair creation process
3. How to handle the results
"""

import json
import os
from create_key_pair import process_usernames

def create_sample_username_file():
    """Create a sample username.json file."""
    usernames = {
        "usernames": [
            "jianhuanggo",
            "jianhuanggo2",
        ]
    }
    
    # Create _example directory if it doesn't exist
    os.makedirs("_example", exist_ok=True)
    
    # Write the usernames to a JSON file
    with open("_example/username.json", "w") as f:
        json.dump(usernames, f, indent=4)
    
    print("Created sample username.json file")

def main():
    """Main function demonstrating key pair creation."""
    # Create sample username file
    create_sample_username_file()
    
    # Create output directory for keys
    output_dir = "_example/keys"
    os.makedirs(output_dir, exist_ok=True)
    
    # Process the usernames and create key pairs
    try:
        results = process_usernames(
            username_json_path="_example/username.json",
            output_dir=output_dir
        )
        
        # Print results
        print("\nKey pair creation results:")
        print("-" * 50)
        for result in results:
            print(f"\nUsername: {result['username']}")
            print(f"Private key path: {result['private_key_path']}")
            print(f"Public key path: {result['public_key_path']}")
            print(f"Secret ARN: {result['secret_arn']}")
        
        print(f"\nSuccessfully processed {len(results)} users")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 