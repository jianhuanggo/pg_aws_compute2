"""
Test suite for launch template creation and management.

This module contains tests for:
1. Creating launch templates with proper naming and configuration
2. Setting up default AMI and instance type
3. Deleting launch templates
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
from create_launch_template import (
    create_launch_template,
    delete_launch_template,
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
def sample_resources():
    """Create sample resources needed for launch template."""
    with mock_ec2():
        session = boto3.Session(profile_name='latest')
        ec2_client = session.client('ec2')
        
        # Create security group
        sg_response = ec2_client.create_security_group(
            GroupName='test-sg',
            Description='Test security group'
        )
        security_group_id = sg_response['GroupId']
        
        # Create key pair
        key_response = ec2_client.create_key_pair(KeyName='test-key')
        key_name = key_response['KeyName']
        
        # Create IAM role (mocked)
        iam_role_arn = 'arn:aws:iam::123456789012:role/test-role'
        
        # Create KMS key (mocked)
        kms_key_id = 'arn:aws:kms:us-east-1:123456789012:key/test-key'
        
        return {
            'security_group_id': security_group_id,
            'key_name': key_name,
            'iam_role_arn': iam_role_arn,
            'kms_key_id': kms_key_id
        }

def test_generate_random_suffix():
    """Test random suffix generation."""
    suffix = generate_random_suffix()
    assert len(suffix) == 6
    assert all(c in string.ascii_lowercase + string.digits for c in suffix)

@mock_ec2
def test_create_launch_template(mock_aws_credentials, sample_resources):
    """Test creating a launch template with default configuration."""
    user_text = 'testuser'
    user_data_file = 'test_user_data.sh'
    
    # Create test user data file
    with open(user_data_file, 'w') as f:
        f.write('#!/bin/bash\necho "Test user data"')
    
    try:
        # Create launch template
        template_id = create_launch_template(
            user_text=user_text,
            security_group_id=sample_resources['security_group_id'],
            key_name=sample_resources['key_name'],
            iam_role_arn=sample_resources['iam_role_arn'],
            user_data_file=user_data_file,
            kms_key_id=sample_resources['kms_key_id']
        )
        
        # Verify launch template exists and has correct name format
        ec2_client = boto3.client('ec2')
        response = ec2_client.describe_launch_templates(
            LaunchTemplateIds=[template_id]
        )
        template = response['LaunchTemplates'][0]
        
        assert template['LaunchTemplateId'] == template_id
        assert template['LaunchTemplateName'].startswith('lt_-testuser-')
        assert len(template['LaunchTemplateName']) == len('lt_-testuser-') + 6
        
        # Verify default configuration
        version = ec2_client.describe_launch_template_versions(
            LaunchTemplateId=template_id,
            Versions=['$Latest']
        )['LaunchTemplateVersions'][0]
        
        assert version['LaunchTemplateData']['InstanceType'] == 't3.large'
        assert 'al2023-ami' in version['LaunchTemplateData']['ImageId']
        assert version['LaunchTemplateData']['SecurityGroupIds'] == [sample_resources['security_group_id']]
        assert version['LaunchTemplateData']['KeyName'] == sample_resources['key_name']
        assert version['LaunchTemplateData']['IamInstanceProfile']['Arn'] == sample_resources['iam_role_arn']
        assert version['LaunchTemplateData']['BlockDeviceMappings'][0]['Ebs']['Encrypted'] is True
        assert version['LaunchTemplateData']['BlockDeviceMappings'][0]['Ebs']['KmsKeyId'] == sample_resources['kms_key_id']
        
    finally:
        # Clean up test user data file
        if os.path.exists(user_data_file):
            os.remove(user_data_file)

@mock_ec2
def test_delete_launch_template(mock_aws_credentials, sample_resources):
    """Test deleting a launch template."""
    user_text = 'testuser'
    user_data_file = 'test_user_data.sh'
    
    # Create test user data file
    with open(user_data_file, 'w') as f:
        f.write('#!/bin/bash\necho "Test user data"')
    
    try:
        # Create launch template
        template_id = create_launch_template(
            user_text=user_text,
            security_group_id=sample_resources['security_group_id'],
            key_name=sample_resources['key_name'],
            iam_role_arn=sample_resources['iam_role_arn'],
            user_data_file=user_data_file,
            kms_key_id=sample_resources['kms_key_id']
        )
        
        # Delete launch template
        delete_launch_template(template_id)
        
        # Verify launch template is deleted
        ec2_client = boto3.client('ec2')
        with pytest.raises(Exception):
            ec2_client.describe_launch_templates(LaunchTemplateIds=[template_id])
            
    finally:
        # Clean up test user data file
        if os.path.exists(user_data_file):
            os.remove(user_data_file)

@mock_ec2
def test_duplicate_launch_template_name(mock_aws_credentials, sample_resources):
    """Test handling duplicate launch template name."""
    user_text = 'testuser'
    user_data_file = 'test_user_data.sh'
    
    # Create test user data file
    with open(user_data_file, 'w') as f:
        f.write('#!/bin/bash\necho "Test user data"')
    
    try:
        # Create first launch template
        create_launch_template(
            user_text=user_text,
            security_group_id=sample_resources['security_group_id'],
            key_name=sample_resources['key_name'],
            iam_role_arn=sample_resources['iam_role_arn'],
            user_data_file=user_data_file,
            kms_key_id=sample_resources['kms_key_id']
        )
        
        # Attempt to create duplicate
        with pytest.raises(Exception):
            create_launch_template(
                user_text=user_text,
                security_group_id=sample_resources['security_group_id'],
                key_name=sample_resources['key_name'],
                iam_role_arn=sample_resources['iam_role_arn'],
                user_data_file=user_data_file,
                kms_key_id=sample_resources['kms_key_id']
            )
            
    finally:
        # Clean up test user data file
        if os.path.exists(user_data_file):
            os.remove(user_data_file)

@mock_ec2
def test_delete_nonexistent_launch_template(mock_aws_credentials):
    """Test handling deletion of non-existent launch template."""
    with pytest.raises(Exception):
        delete_launch_template('lt-nonexistent') 