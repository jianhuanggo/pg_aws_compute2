"""
Test suite for Auto Scaling Group creation and management.

This module contains tests for:
1. Creating ASGs with proper naming and configuration
2. Setting up default instance counts and availability zones
3. Deleting ASGs
"""

import pytest
import boto3
import os
import sys
import string
import random
from moto import mock_autoscaling, mock_ec2

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from create_asg import (
    create_asg,
    delete_asg,
    generate_random_suffix
)

# cursor: include

@pytest.fixture
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def sample_launch_template():
    """Create a sample launch template for testing."""
    with mock_ec2():
        session = boto3.Session(profile_name='latest')
        ec2_client = session.client('ec2')
        
        # Create launch template
        response = ec2_client.create_launch_template(
            LaunchTemplateName='test-template',
            LaunchTemplateData={
                'ImageId': 'ami-12345678',
                'InstanceType': 't3.large'
            }
        )
        return response['LaunchTemplate']['LaunchTemplateId']

def test_generate_random_suffix():
    """Test random suffix generation."""
    suffix = generate_random_suffix()
    assert len(suffix) == 6
    assert all(c in string.ascii_lowercase + string.digits for c in suffix)

@mock_autoscaling
def test_create_asg(mock_aws_credentials, sample_launch_template):
    """Test creating an Auto Scaling Group with default configuration."""
    user_text = 'testuser'
    
    # Create ASG
    asg_name = create_asg(user_text, sample_launch_template)
    
    # Verify ASG exists and has correct name format
    asg_client = boto3.client('autoscaling')
    response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    asg = response['AutoScalingGroups'][0]
    
    assert asg['AutoScalingGroupName'] == asg_name
    assert asg_name.startswith('asg_-testuser-')
    assert len(asg_name) == len('asg_-testuser-') + 6
    
    # Verify default configuration
    assert asg['MinSize'] == 1
    assert asg['MaxSize'] == 1
    assert asg['DesiredCapacity'] == 1
    assert set(asg['AvailabilityZones']) == {'us-east-1a', 'us-east-1b'}
    assert asg['LaunchTemplate']['LaunchTemplateId'] == sample_launch_template

@mock_autoscaling
def test_delete_asg(mock_aws_credentials, sample_launch_template):
    """Test deleting an Auto Scaling Group."""
    user_text = 'testuser'
    
    # Create ASG
    asg_name = create_asg(user_text, sample_launch_template)
    
    # Delete ASG
    delete_asg(asg_name)
    
    # Verify ASG is deleted
    asg_client = boto3.client('autoscaling')
    response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    assert len(response['AutoScalingGroups']) == 0

@mock_autoscaling
def test_duplicate_asg_name(mock_aws_credentials, sample_launch_template):
    """Test handling duplicate ASG name."""
    user_text = 'testuser'
    
    # Create first ASG
    create_asg(user_text, sample_launch_template)
    
    # Attempt to create duplicate
    with pytest.raises(Exception):
        create_asg(user_text, sample_launch_template)

@mock_autoscaling
def test_delete_nonexistent_asg(mock_aws_credentials):
    """Test handling deletion of non-existent ASG."""
    with pytest.raises(Exception):
        delete_asg('asg-nonexistent') 