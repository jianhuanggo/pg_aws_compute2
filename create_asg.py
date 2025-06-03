"""
AWS Auto Scaling Group Management Module

This module provides functionality to create and delete Auto Scaling Groups with default configuration.
It handles ASG creation, configuration, and deletion.

Examples:
    >>> create_asg('myapp', 'lt-12345678')
    'asg_-myapp-abc123'
    
    >>> delete_asg('asg_-myapp-abc123')
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

def create_asg(user_text: str, launch_template_id: str) -> str:
    """
    Create an Auto Scaling Group with default configuration.
    
    Args:
        user_text: User-specific text to include in the ASG name
        launch_template_id: ID of the launch template to use
        
    Returns:
        str: The created ASG name
        
    Raises:
        ClientError: If there's an error creating the ASG
        Exception: If the ASG name already exists
    """
    session = boto3.Session(profile_name='latest')
    asg_client = session.client('autoscaling')
    
    asg_name = f"asg_-{user_text}-{generate_random_suffix()}"
    
    try:
        # Create Auto Scaling Group
        asg_client.create_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            LaunchTemplate={
                'LaunchTemplateId': launch_template_id,
                'Version': '$Latest'
            },
            MinSize=1,
            MaxSize=1,
            DesiredCapacity=1,
            AvailabilityZones=['us-east-1a', 'us-east-1b'],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': asg_name,
                    'PropagateAtLaunch': True
                }
            ]
        )
        
        return asg_name
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExists':
            raise Exception(f"Auto Scaling Group {asg_name} already exists")
        raise

def delete_asg(asg_name: str) -> None:
    """
    Delete an Auto Scaling Group.
    
    Args:
        asg_name: Name of the Auto Scaling Group to delete
        
    Raises:
        ClientError: If there's an error deleting the ASG
        Exception: If the ASG doesn't exist
    """
    session = boto3.Session(profile_name='latest')
    asg_client = session.client('autoscaling')
    
    try:
        # Delete Auto Scaling Group
        asg_client.delete_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            ForceDelete=True  # Force delete even if instances are running
        )
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'AutoScalingGroupNameNotFound':
            raise Exception(f"Auto Scaling Group {asg_name} does not exist")
        raise

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Create or delete Auto Scaling Group')
    parser.add_argument('action', choices=['create', 'delete'], help='Action to perform')
    parser.add_argument('--user-text', help='User-specific text for naming (required for create)')
    parser.add_argument('--launch-template-id', help='Launch template ID (required for create)')
    parser.add_argument('--asg-name', help='Auto Scaling Group name (required for delete)')
    
    args = parser.parse_args()
    
    try:
        if args.action == 'create':
            if not args.user_text or not args.launch_template_id:
                parser.error("--user-text and --launch-template-id are required for create action")
            
            asg_name = create_asg(args.user_text, args.launch_template_id)
            print(f"Created Auto Scaling Group: {asg_name}")
            
        else:  # delete
            if not args.asg_name:
                parser.error("--asg-name is required for delete action")
            
            delete_asg(args.asg_name)
            print(f"Deleted Auto Scaling Group: {args.asg_name}")
            
    except Exception as e:
        print(f"Error: {str(e)}") 