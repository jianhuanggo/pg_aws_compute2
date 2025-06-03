"""
AWS IAM Role and Policy Management Module

This module provides functionality to create IAM roles and policies for KMS key access.
It handles role creation, policy creation, and policy attachment.

Examples:
    >>> create_iam_role('myapp')
    'iam-role-myapp-abc123'
    
    >>> create_kms_policy('myapp', 'arn:aws:kms:region:account:key/123')
    'iam-policy-myapp-xyz789'
    
    >>> attach_policy_to_role('iam-role-myapp-abc123', 'iam-policy-myapp-xyz789')
"""

import boto3
import string
import json
import random
from typing import Dict
from botocore.exceptions import ClientError

def generate_random_suffix() -> str:
    """
    Generate a random 6-character suffix using lowercase letters and numbers.
    
    Returns:
        str: Random 6-character string
    """
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(6))

def create_iam_role(user_text: str) -> str:
    """
    Create an IAM role with the specified user text in the name.
    
    Args:
        user_text: User-specific text to include in the role name
        
    Returns:
        str: The created role name
        
    Raises:
        ClientError: If there's an error creating the role
        Exception: If the role name already exists
    """
    session = boto3.Session(profile_name='latest')
    iam_client = session.client('iam')
    
    role_name = f"iam-role-{user_text}-{generate_random_suffix()}"
    
    try:
        # Create the role with a trust policy that allows EC2 to assume the role
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ec2.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )

        
        return role_name
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            raise Exception(f"Role {role_name} already exists")
        raise

def create_kms_policy(user_text: str, kms_key_arn: str) -> str:
    """
    Create an IAM policy that grants access to a specific KMS key.
    
    Args:
        user_text: User-specific text to include in the policy name
        kms_key_arn: ARN of the KMS key to grant access to
        
    Returns:
        str: The created policy name
        
    Raises:
        ClientError: If there's an error creating the policy
        Exception: If the policy name already exists
    """
    session = boto3.Session(profile_name='latest')
    iam_client = session.client('iam')
    
    policy_name = f"iam-policy-{user_text}-{generate_random_suffix()}"
    
    try:
        # Create policy document that grants KMS access
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey"
                    ],
                    "Resource": kms_key_arn
                }
            ]
        }
        
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        
        return policy_name
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            raise Exception(f"Policy {policy_name} already exists")
        raise

def attach_policy_to_role(role_name: str, policy_name: str) -> None:
    """
    Attach an IAM policy to a role.
    
    Args:
        role_name: Name of the IAM role
        policy_name: Name of the IAM policy
        
    Raises:
        ClientError: If there's an error attaching the policy
        Exception: If the role or policy doesn't exist
    """
    session = boto3.Session(profile_name='latest')
    iam_client = session.client('iam')
    
    try:
        # Get the policy ARN
        policy_arn = f"arn:aws:iam::{session.client('sts').get_caller_identity()['Account']}:policy/{policy_name}"
        
        # Attach the policy to the role
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            raise Exception(f"Role {role_name} or policy {policy_name} does not exist")
        raise

if __name__ == '__main__':
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Create IAM role and policy for KMS access')
    parser.add_argument('user_text', help='User-specific text for naming')
    parser.add_argument('kms_key_arn', help='ARN of the KMS key to grant access to')
    
    args = parser.parse_args()
    
    try:
        # Create role
        role_name = create_iam_role(args.user_text)
        print(f"Created IAM role: {role_name}")
        
        # Create policy
        policy_name = create_kms_policy(args.user_text, args.kms_key_arn)
        print(f"Created IAM policy: {policy_name}")
        
        # Attach policy to role
        attach_policy_to_role(role_name, policy_name)
        print(f"Attached policy {policy_name} to role {role_name}")
        
    except Exception as e:
        print(f"Error: {str(e)}") 