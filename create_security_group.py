"""
AWS Security Group Management Module

This module provides functionality to create and delete security groups with default rules.
It handles security group creation, rule configuration, and deletion.

Examples:
    >>> create_security_group('myapp', 'vpc-12345678')
    'sg-abc123def456'
    
    >>> delete_security_group('sg-abc123def456')
"""

import boto3
import string
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

def create_security_group(user_text: str, vpc_id: str) -> str:
    """
    Create a security group with default rules allowing all traffic from the same group.
    
    Args:
        user_text: User-specific text to include in the security group name
        vpc_id: ID of the VPC to create the security group in
        
    Returns:
        str: The created security group ID
        
    Raises:
        ClientError: If there's an error creating the security group
        Exception: If the security group name already exists
    """
    session = boto3.Session(profile_name='latest')
    ec2_client = session.client('ec2')
    
    group_name = f"sg_-{user_text}-{generate_random_suffix()}"
    
    try:
        # Create security group
        response = ec2_client.create_security_group(
            GroupName=group_name,
            Description=f"Security group for {user_text}",
            VpcId=vpc_id
        )
        group_id = response['GroupId']
        
        # Add default rule allowing all traffic from the same security group
        ec2_client.authorize_security_group_ingress(
            GroupId=group_id,
            IpPermissions=[
                {
                    'IpProtocol': '-1',  # All protocols
                    'UserIdGroupPairs': [
                        {
                            'GroupId': group_id,
                            'Description': 'Allow all traffic from same security group'
                        }
                    ]
                }
            ]
        )
        
        return group_id
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            raise Exception(f"Security group {group_name} already exists")
        raise

def delete_security_group(group_id: str) -> None:
    """
    Delete a security group.
    
    Args:
        group_id: ID of the security group to delete
        
    Raises:
        ClientError: If there's an error deleting the security group
        Exception: If the security group doesn't exist
    """
    session = boto3.Session(profile_name='latest')
    ec2_client = session.client('ec2')
    
    try:
        ec2_client.delete_security_group(GroupId=group_id)
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.NotFound':
            raise Exception(f"Security group {group_id} does not exist")
        raise

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Create or delete security group')
    parser.add_argument('action', choices=['create', 'delete'], help='Action to perform')
    parser.add_argument('--user-text', help='User-specific text for naming (required for create)')
    parser.add_argument('--vpc-id', help='VPC ID (required for create)')
    parser.add_argument('--group-id', help='Security group ID (required for delete)')
    
    args = parser.parse_args()
    
    try:
        if args.action == 'create':
            if not args.user_text or not args.vpc_id:
                parser.error("--user-text and --vpc-id are required for create action")
            
            group_id = create_security_group(args.user_text, args.vpc_id)
            print(f"Created security group: {group_id}")
            
        else:  # delete
            if not args.group_id:
                parser.error("--group-id is required for delete action")
            
            delete_security_group(args.group_id)
            print(f"Deleted security group: {args.group_id}")
            
    except Exception as e:
        print(f"Error: {str(e)}") 