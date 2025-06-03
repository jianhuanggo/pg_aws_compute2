"""
AWS Launch Template Management Module

This module provides functionality to create and delete launch templates with default configuration.
It handles launch template creation, configuration, and deletion.

Examples:
    >>> create_launch_template('myapp', 'sg-123', 'key-123', 'role-123', 'userdata.sh', 'kms-123')
    'lt-abc123def456'
    
    >>> delete_launch_template('lt-abc123def456')
"""

import boto3
import string
import random
import base64
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

def get_latest_amazon_linux_2023_ami() -> str:
    """
    Get the latest Amazon Linux 2023 AMI ID.
    
    Returns:
        str: AMI ID for the latest Amazon Linux 2023 x86_64 AMI
    """
    session = boto3.Session(profile_name='latest')
    ec2_client = session.client('ec2')
    
    response = ec2_client.describe_images(
        Filters=[
            {
                'Name': 'name',
                'Values': ['al2023-ami-*-x86_64']
            },
            {
                'Name': 'state',
                'Values': ['available']
            },
            {
                'Name': 'architecture',
                'Values': ['x86_64']
            }
        ],
        Owners=['amazon']
    )
    
    # Sort by creation date and get the latest
    images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
    return images[0]['ImageId']

def create_launch_template(
    user_text: str,
    security_group_id: str,
    key_name: str,
    iam_role_arn: str,
    user_data_file: str,
    kms_key_id: str
) -> str:
    """
    Create a launch template with default configuration.
    
    Args:
        user_text: User-specific text to include in the template name
        security_group_id: ID of the security group to use
        key_name: Name of the key pair to use
        iam_role_arn: ARN of the IAM role to use
        user_data_file: Path to the user data script file
        kms_key_id: ID of the KMS key for EBS encryption
        
    Returns:
        str: The created launch template ID
        
    Raises:
        ClientError: If there's an error creating the launch template
        Exception: If the launch template name already exists
    """
    session = boto3.Session(profile_name='latest')
    ec2_client = session.client('ec2')
    
    template_name = f"lt_-{user_text}-{generate_random_suffix()}"
    
    try:
        # Read user data file
        with open(user_data_file, 'r') as f:
            user_data = f.read()
        
        # Get latest Amazon Linux 2023 AMI
        ami_id = get_latest_amazon_linux_2023_ami()
        
        # Create launch template
        response = ec2_client.create_launch_template(
            LaunchTemplateName=template_name,
            VersionDescription='Initial version',
            LaunchTemplateData={
                'ImageId': ami_id,
                'InstanceType': 't3.large',
                'SecurityGroupIds': [security_group_id],
                'KeyName': key_name,
                'IamInstanceProfile': {
                    'Arn': iam_role_arn
                },
                'UserData': base64.b64encode(user_data.encode()).decode(),
                'BlockDeviceMappings': [
                    {
                        'DeviceName': '/dev/xvda',
                        'Ebs': {
                            'VolumeSize': 8,
                            'VolumeType': 'gp3',
                            'Encrypted': True,
                            'KmsKeyId': kms_key_id
                        }
                    }
                ]
            }
        )
        
        return response['LaunchTemplate']['LaunchTemplateId']
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidLaunchTemplateName.AlreadyExistsException':
            raise Exception(f"Launch template {template_name} already exists")
        raise

def delete_launch_template(template_id: str) -> None:
    """
    Delete a launch template.
    
    Args:
        template_id: ID of the launch template to delete
        
    Raises:
        ClientError: If there's an error deleting the launch template
        Exception: If the launch template doesn't exist
    """
    session = boto3.Session(profile_name='latest')
    ec2_client = session.client('ec2')
    
    try:
        ec2_client.delete_launch_template(LaunchTemplateId=template_id)
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidLaunchTemplateId.NotFound':
            raise Exception(f"Launch template {template_id} does not exist")
        raise

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Create or delete launch template')
    parser.add_argument('action', choices=['create', 'delete'], help='Action to perform')
    parser.add_argument('--user-text', help='User-specific text for naming (required for create)')
    parser.add_argument('--security-group-id', help='Security group ID (required for create)')
    parser.add_argument('--key-name', help='Key pair name (required for create)')
    parser.add_argument('--iam-role-arn', help='IAM role ARN (required for create)')
    parser.add_argument('--user-data-file', help='Path to user data script file (required for create)')
    parser.add_argument('--kms-key-id', help='KMS key ID for EBS encryption (required for create)')
    parser.add_argument('--template-id', help='Launch template ID (required for delete)')
    
    args = parser.parse_args()
    
    try:
        if args.action == 'create':
            if not all([args.user_text, args.security_group_id, args.key_name,
                       args.iam_role_arn, args.user_data_file, args.kms_key_id]):
                parser.error("All create parameters are required")
            
            template_id = create_launch_template(
                args.user_text,
                args.security_group_id,
                args.key_name,
                args.iam_role_arn,
                args.user_data_file,
                args.kms_key_id
            )
            print(f"Created launch template: {template_id}")
            
        else:  # delete
            if not args.template_id:
                parser.error("--template-id is required for delete action")
            
            delete_launch_template(args.template_id)
            print(f"Deleted launch template: {args.template_id}")
            
    except Exception as e:
        print(f"Error: {str(e)}") 