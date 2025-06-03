"""
AWS KMS Key Management Module

This module provides functionality to create and manage AWS KMS keys.
It handles key creation, tagging, and key details retrieval.

Examples:
    >>> create_kms_key('my-key', 'My KMS key')
    {'KeyId': '...', 'KeyArn': '...', 'AliasName': 'alias/my-key'}
    
    >>> get_kms_key_details('my-key')
    {'KeyId': '...', 'KeyArn': '...', 'AliasName': 'alias/my-key'}
"""

import boto3
from typing import Dict, List, Optional
from botocore.exceptions import ClientError

def create_kms_key(
    key_alias: str,
    description: str,
    tags: Optional[List[Dict[str, str]]] = None
) -> Dict:
    """
    Create an AWS KMS key with the specified alias and description.
    
    Args:
        key_alias: The alias for the KMS key
        description: Description of the key's purpose
        tags: Optional list of tags to apply to the key
        
    Returns:
        Dict containing the key details including KeyId, KeyArn, and AliasName
        
    Raises:
        ClientError: If there's an error creating the key
        Exception: If the key alias already exists
    """
    session = boto3.Session(profile_name='latest')
    kms_client = session.client('kms')
    
    try:
        # Create the key
        response = kms_client.create_key(
            Description=description,
            KeyUsage='ENCRYPT_DECRYPT',
            Origin='AWS_KMS',
            Tags=tags or []
        )
        
        key_id = response['KeyMetadata']['KeyId']
        
        # Create the alias
        kms_client.create_alias(
            AliasName=f"alias/{key_alias}",
            TargetKeyId=key_id
        )
        
        return {
            'KeyId': key_id,
            'KeyArn': response['KeyMetadata']['Arn'],
            'AliasName': f"alias/{key_alias}"
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            raise Exception(f"Key alias 'alias/{key_alias}' already exists")
        raise

def get_kms_key_details(key_alias: str) -> Dict:
    """
    Get details of an existing KMS key by its alias.
    
    Args:
        key_alias: The alias of the KMS key
        
    Returns:
        Dict containing the key details including KeyId, KeyArn, and AliasName
        
    Raises:
        ClientError: If there's an error retrieving the key
        Exception: If the key alias doesn't exist
    """
    session = boto3.Session(profile_name='latest')
    kms_client = session.client('kms')
    
    try:
        # List aliases to find the key ID
        response = kms_client.list_aliases()
        alias_name = f"alias/{key_alias}"
        
        for alias in response['Aliases']:
            if alias['AliasName'] == alias_name:
                key_id = alias['TargetKeyId']
                
                # Get key details
                key_response = kms_client.describe_key(KeyId=key_id)
                key_metadata = key_response['KeyMetadata']
                
                return {
                    'KeyId': key_id,
                    'KeyArn': key_metadata['Arn'],
                    'AliasName': alias_name
                }
        
        raise Exception(f"Key alias '{key_alias}' not found")
        
    except ClientError as e:
        raise Exception(f"Error retrieving key details: {str(e)}")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Create AWS KMS key')
    parser.add_argument('key_alias', help='Alias for the KMS key')
    parser.add_argument('--description', default='KMS key for encryption/decryption',
                      help='Description of the key')
    parser.add_argument('--tags', nargs='+', help='Tags in format "Key=Value"')
    
    args = parser.parse_args()
    
    # Parse tags if provided
    tags = None
    if args.tags:
        tags = []
        for tag in args.tags:
            key, value = tag.split('=')
            tags.append({'TagKey': key, 'TagValue': value})
    
    try:
        key_details = create_kms_key(args.key_alias, args.description, tags)
        print(f"Successfully created KMS key:")
        print(f"Key ID: {key_details['KeyId']}")
        print(f"Key ARN: {key_details['KeyArn']}")
        print(f"Alias: {key_details['AliasName']}")
    except Exception as e:
        print(f"Error: {str(e)}") 