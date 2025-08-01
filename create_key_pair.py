"""
AWS Key Pair Management Module

This module provides functionality to create and manage AWS key pairs for multiple users.
It handles key pair creation, file storage, and secrets management.

Examples:
    >>> process_usernames('username.json', './keys')
    [{'username': 'user1', 'private_key_path': './keys/user1.pem', ...}]
    
    >>> create_key_pair('testuser')
    {'KeyName': 'testuser-key', 'KeyMaterial': '...', 'KeyPairId': '...'}
"""

import boto3
import json
import os
from typing import Dict, List, Tuple, Optional
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def create_key_pair(profile_name: str, key_name: str) -> Dict:
    """
    Create an AWS key pair for the given username.
    
    Args:
        profile_name: aws profile name
        username: The username to create the key pair for
        
    Returns:
        Dict containing the key pair information
        
    Raises:
        ClientError: If there's an error creating the key pair
    """
    session = boto3.Session(profile_name=profile_name)
    ec2_client = session.client('ec2')
    
    try:
        response = ec2_client.create_key_pair(
            KeyName=key_name,
            KeyType='rsa',
            KeyFormat='pem'
        )
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidKeyPair.Duplicate':
            raise Exception(f"Key pair for {key_name} already exists")
        raise


def delete_key_pair(profile_name: str, key_name: str) -> Dict:
    """
    Create an AWS key pair for the given username.

    Args:
        profile_name: aws profile name
        key_name: The username to create the key pair for

    Returns:
        Dict containing the key pair information

    Raises:
        ClientError: If there's an error creating the key pair
    """
    session = boto3.Session(profile_name=profile_name)
    ec2_client = session.client('ec2')

    try:
        response = ec2_client.delete_key_pair(KeyName=key_name)
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidKeyPair.NotFound':
            print("key is not found, continue")
        else:
            raise Exception(f"Encounter {e}")


def save_key_pair_to_file(
    private_key: str,
    public_key: str,
    username: str,
    output_dir: str
) -> Tuple[str, str]:
    """
    Save the key pair to files.
    
    Args:
        private_key: The private key material
        public_key: The public key material
        username: The username associated with the keys
        output_dir: Directory to save the keys
        
    Returns:
        Tuple of (private_key_path, public_key_path)
    """
    os.makedirs(output_dir, exist_ok=True)
    
    private_path = os.path.join(output_dir, f"{username}.pem")
    public_path = os.path.join(output_dir, f"{username}.pub")
    
    with open(private_path, 'w') as f:
        f.write(private_key)
    
    with open(public_path, 'w') as f:
        f.write(public_key)
    
    # Set proper permissions for private key
    os.chmod(private_path, 0o600)
    
    return private_path, public_path


def save_to_secrets_manager(
    profile_name: str,
    secret_name: str,
    private_key: str,
    public_key: str
) -> str:
    """
    Save the key pair to AWS Secrets Manager.
    
    Args:
        username: The username associated with the keys
        private_key: The private key material
        public_key: The public key material
        
    Returns:
        The ARN of the created secret
    """
    session = boto3.Session(profile_name=profile_name)
    secrets_client = session.client('secretsmanager')
    
    secret_value = {
        'private_key': private_key,
        'public_key': public_key
    }

    
    try:
        response = secrets_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value)
        )
        return response['ARN']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceExistsException':
            # Update existing secret
            response = secrets_client.update_secret(
                SecretId=secret_name,
                SecretString=json.dumps(secret_value)
            )
            return response['ARN']
        raise


def delete_secrets_manager(
        profile_name: str,
        secret_name: str,
        force_delete=False,
        recovery_window_in_days=30) -> dict:
    """
    Save the key pair to AWS Secrets Manager.

    Args:
        profile_name: profile name
        secret_name: secret name
        force_delete: The private key material
        recovery_window_in_days: The public key material

    Returns:
        The ARN of the created secret
    """
    session = boto3.Session(profile_name=profile_name)
    secrets_client = session.client('secretsmanager')

    try:
        if force_delete:
            response = secrets_client.delete_secret(
                SecretId=secret_name,
                ForceDeleteWithoutRecovery=True
            )
        else:
            response = secrets_client.delete_secret(
                SecretId=secret_name,
                RecoveryWindowInDays=recovery_window_in_days
            )
        return {
            "Status": "Success",
            "DeletionDate": response.get("DeletionDate"),
            "ARN": response.get("ARN"),
            "Name": response.get("Name")
        }
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceExistsException':
            print(f"resource {secret_name} not found")
        else:
            raise e


def process_usernames(profile_name: str, username_json_path: str, output_dir: str, user_context: str="") -> List[Dict]:
    """
    Process multiple usernames from a JSON file and create key pairs for each.
    
    Args:
        profile_name: aws profile name
        username_json_path: Path to the JSON file containing usernames
        user_context: Context to add to the key pair name
        output_dir: Directory to save the key files
        
    Returns:
        List of dictionaries containing processing results for each username
        
    Raises:
        FileNotFoundError: If username.json doesn't exist
        json.JSONDecodeError: If username.json is invalid
    """
    try:
        with open(username_json_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        raise Exception(f"Username file not found: {username_json_path}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON in file: {username_json_path}")
    
    if 'usernames' not in data:
        raise Exception("JSON file must contain 'usernames' key")
    
    results = []
    for username in data['usernames']:
        try:
            # Create key pair
            key_pair = create_key_pair(profile_name, f"kp-{user_context}-{username}")

            # Extract public key
            load_private_key = serialization.load_pem_private_key(key_pair['KeyMaterial'].encode('utf-8'),
                                                                  password=None,
                                                                  backend=default_backend())
            public_key = load_private_key.public_key()
            public_key_pem = public_key.public_bytes(
                encoding=serialization.Encoding.OpenSSH,
                format=serialization.PublicFormat.OpenSSH
            )

            # Save to files
            private_path, public_path = save_key_pair_to_file(
                key_pair['KeyMaterial'],
                public_key_pem.decode('utf8'),  # AWS provides the same material for both
                f"kp-{user_context}-{username}",
                output_dir
            )
            
            # Save to Secrets Manager
            secret_arn = save_to_secrets_manager(
                profile_name,
                f"ssm-kp-{user_context}-{username}",
                key_pair['KeyMaterial'],
                public_key_pem.decode('utf8')
            )
            
            results.append({
                'username': username,
                'private_key_path': private_path,
                'public_key_path': public_path,
                'secret_arn': secret_arn
            })
            
        except Exception as e:
            print(f"Error processing username {username}: {str(e)}")
            exit(0)
            continue
    
    return results


def delete_credential(profile_name: str, username_json_path: str, user_context: str = "") -> List[Dict]:
    """
    Process multiple usernames from a JSON file and delete key pairs and corresponding secret key for each.

    Args:
        profile_name: aws profile name
        username_json_path: Path to the JSON file containing usernames
        user_context: Context to add to the key pair name

    Returns:
        List of dictionaries containing processing results for each username

    Raises:
        FileNotFoundError: If username.json doesn't exist
        json.JSONDecodeError: If username.json is invalid
    """

    try:
        with open(username_json_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        raise Exception(f"Username file not found: {username_json_path}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON in file: {username_json_path}")

    if 'usernames' not in data:
        raise Exception("JSON file must contain 'usernames' key")

    results = []
    for username in data['usernames']:
        try:
            # Delete key pair
            key_pair = delete_key_pair(profile_name, f"kp-{user_context}-{username}")

            # Save to Secrets Manager
            secret_arn = delete_secrets_manager(
                profile_name,
                secret_name=f"ssm-kp-{user_context}-{username}",
                force_delete=True
            )

        except Exception as e:
            print(f"Error processing username {username}: {str(e)}")
            exit(0)
            continue

    return results


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Create AWS key pairs for multiple users')
    parser.add_argument('username_json', help='Path to username.json file')
    parser.add_argument('--output-dir', default='./keys', help='Directory to save key files')
    
    args = parser.parse_args()
    
    results = process_usernames(args.username_json, args.output_dir)
    print(f"Processed {len(results)} users successfully") 