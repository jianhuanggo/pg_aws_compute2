import pytest
import boto3
import json
import os
import sys
from moto import mock_kms

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from create_kms import create_kms_key, get_kms_key_details

# cursor: include

@pytest.fixture
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@mock_kms
def test_create_kms_key(mock_aws_credentials):
    """Test creating a KMS key."""
    key_alias = 'test-key'
    description = 'Test KMS key'
    
    key_details = create_kms_key(key_alias, description)
    
    assert key_details is not None
    assert 'KeyId' in key_details
    assert 'KeyArn' in key_details
    assert 'AliasName' in key_details
    assert key_details['AliasName'] == f"alias/{key_alias}"

@mock_kms
def test_create_kms_key_with_tags(mock_aws_credentials):
    """Test creating a KMS key with tags."""
    key_alias = 'test-key-tagged'
    description = 'Test KMS key with tags'
    tags = [
        {'TagKey': 'Environment', 'TagValue': 'Test'},
        {'TagKey': 'Project', 'TagValue': 'KMS-Test'}
    ]
    
    key_details = create_kms_key(key_alias, description, tags)
    
    assert key_details is not None
    assert 'KeyId' in key_details
    assert 'KeyArn' in key_details
    assert 'AliasName' in key_details
    assert key_details['AliasName'] == f"alias/{key_alias}"

@mock_kms
def test_duplicate_key_alias(mock_aws_credentials):
    """Test handling duplicate key alias."""
    key_alias = 'test-duplicate'
    description = 'Test duplicate key'
    
    # Create first key
    create_kms_key(key_alias, description)
    
    # Attempt to create duplicate
    with pytest.raises(Exception):
        create_kms_key(key_alias, description)

@mock_kms
def test_get_kms_key_details(mock_aws_credentials):
    """Test retrieving KMS key details."""
    key_alias = 'test-key-details'
    description = 'Test key details'
    
    # Create a key first
    created_key = create_kms_key(key_alias, description)
    
    # Get key details
    key_details = get_kms_key_details(key_alias)
    
    assert key_details is not None
    assert key_details['KeyId'] == created_key['KeyId']
    assert key_details['KeyArn'] == created_key['KeyArn']
    assert key_details['AliasName'] == created_key['AliasName']

@mock_kms
def test_get_nonexistent_key_details(mock_aws_credentials):
    """Test retrieving details for a nonexistent key."""
    with pytest.raises(Exception):
        get_kms_key_details('nonexistent-key') 