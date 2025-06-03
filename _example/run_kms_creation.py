"""
Example script demonstrating how to use the KMS key creation functionality.

This script shows:
1. How to create a KMS key with basic settings
2. How to create a KMS key with tags
3. How to retrieve key details
"""

from create_kms import create_kms_key, get_kms_key_details

def create_basic_key():
    """Create a basic KMS key."""
    print("\nCreating basic KMS key...")
    try:
        key_details = create_kms_key(
            key_alias='example-key',
            description='Example KMS key for demonstration'
        )
        print("Successfully created basic key:")
        print(f"Key ID: {key_details['KeyId']}")
        print(f"Key ARN: {key_details['KeyArn']}")
        print(f"Alias: {key_details['AliasName']}")
    except Exception as e:
        print(f"Error creating basic key: {str(e)}")

def create_tagged_key():
    """Create a KMS key with tags."""
    print("\nCreating tagged KMS key...")
    try:
        tags = [
            {'TagKey': 'Environment', 'TagValue': 'Development'},
            {'TagKey': 'Project', 'TagValue': 'KMS-Example'},
            {'TagKey': 'Owner', 'TagValue': 'DevOps'}
        ]
        
        key_details = create_kms_key(
            key_alias='example-tagged-key',
            description='Example KMS key with tags',
            tags=tags
        )
        print("Successfully created tagged key:")
        print(f"Key ID: {key_details['KeyId']}")
        print(f"Key ARN: {key_details['KeyArn']}")
        print(f"Alias: {key_details['AliasName']}")
    except Exception as e:
        print(f"Error creating tagged key: {str(e)}")

def retrieve_key_details():
    """Retrieve details of an existing key."""
    print("\nRetrieving key details...")
    try:
        key_details = get_kms_key_details('example-key')
        print("Successfully retrieved key details:")
        print(f"Key ID: {key_details['KeyId']}")
        print(f"Key ARN: {key_details['KeyArn']}")
        print(f"Alias: {key_details['AliasName']}")
    except Exception as e:
        print(f"Error retrieving key details: {str(e)}")

def main():
    """Main function demonstrating KMS key operations."""
    print("KMS Key Creation Example")
    print("=" * 50)
    
    # Create a basic key
    create_basic_key()
    
    # Create a tagged key
    create_tagged_key()
    
    # Retrieve key details
    retrieve_key_details()

if __name__ == "__main__":
    main() 