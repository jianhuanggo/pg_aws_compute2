"""
Test suite for security group creation and management.

This module contains tests for:
1. Creating security groups with proper naming
2. Setting up default security rules
3. Deleting security groups
"""

import pytest
import boto3
import os
import sys
import string
import random
from moto import mock_ec2

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from create_security_group import (
    create_security_group,
    delete_security_group,
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

def test_generate_random_suffix():
    """Test random suffix generation."""
    suffix = generate_random_suffix()
    assert len(suffix) == 6
    assert all(c in string.ascii_lowercase + string.digits for c in suffix)

@mock_ec2
def test_create_security_group(mock_aws_credentials):
    """Test creating a security group with default rules."""
    user_text = 'testuser'
    vpc_id = 'vpc-12345678'  # Mock VPC ID
    
    # Create security group
    sg_id = create_security_group(user_text, vpc_id)
    
    # Verify security group exists and has correct name format
    ec2_client = boto3.client('ec2')
    response = ec2_client.describe_security_groups(GroupIds=[sg_id])
    sg = response['SecurityGroups'][0]
    
    assert sg['GroupId'] == sg_id
    assert sg['GroupName'].startswith('sg_-testuser-')
    assert len(sg['GroupName']) == len('sg_-testuser-') + 6
    
    # Verify default rules
    assert len(sg['IpPermissions']) == 1
    rule = sg['IpPermissions'][0]
    assert rule['IpProtocol'] == '-1'  # All protocols
    assert rule['UserIdGroupPairs'][0]['GroupId'] == sg_id  # Self-referencing

@mock_ec2
def test_delete_security_group(mock_aws_credentials):
    """Test deleting a security group."""
    user_text = 'testuser'
    vpc_id = 'vpc-12345678'  # Mock VPC ID
    
    # Create security group
    sg_id = create_security_group(user_text, vpc_id)
    
    # Delete security group
    delete_security_group(sg_id)
    
    # Verify security group is deleted
    ec2_client = boto3.client('ec2')
    with pytest.raises(Exception):
        ec2_client.describe_security_groups(GroupIds=[sg_id])

@mock_ec2
def test_duplicate_security_group_name(mock_aws_credentials):
    """Test handling duplicate security group name."""
    user_text = 'testuser'
    vpc_id = 'vpc-12345678'  # Mock VPC ID
    
    # Create first security group
    create_security_group(user_text, vpc_id)
    
    # Attempt to create duplicate
    with pytest.raises(Exception):
        create_security_group(user_text, vpc_id)

@mock_ec2
def test_delete_nonexistent_security_group(mock_aws_credentials):
    """Test handling deletion of non-existent security group."""
    with pytest.raises(Exception):
        delete_security_group('sg-nonexistent') 