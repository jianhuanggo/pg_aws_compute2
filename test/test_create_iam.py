import pytest
import boto3
import json
import os
import sys
import string
import random
from moto import mock_iam, mock_kms

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from create_iam import (
    create_iam_role,
    create_kms_policy,
    attach_policy_to_role,
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
def sample_kms_key():
    """Create a sample KMS key for testing."""
    with mock_kms():
        session = boto3.Session(profile_name='latest')
        kms_client = session.client('kms')
        response = kms_client.create_key(
            Description='Test KMS key',
            KeyUsage='ENCRYPT_DECRYPT'
        )
        return response['KeyMetadata']['Arn']

def test_generate_random_suffix():
    """Test random suffix generation."""
    suffix = generate_random_suffix()
    assert len(suffix) == 6
    assert all(c in string.ascii_lowercase + string.digits for c in suffix)

@mock_iam
def test_create_iam_role(mock_aws_credentials):
    """Test creating an IAM role."""
    user_text = 'testuser'
    role_name = create_iam_role(user_text)
    
    assert role_name.startswith('iam-role-testuser-')
    assert len(role_name) == len('iam-role-testuser-') + 6
    
    # Verify role exists
    iam_client = boto3.client('iam')
    role = iam_client.get_role(RoleName=role_name)
    assert role['Role']['RoleName'] == role_name

@mock_iam
def test_create_kms_policy(mock_aws_credentials, sample_kms_key):
    """Test creating a KMS policy."""
    user_text = 'testuser'
    policy_name = create_kms_policy(user_text, sample_kms_key)
    
    assert policy_name.startswith('iam-policy-testuser-')
    assert len(policy_name) == len('iam-policy-testuser-') + 6
    
    # Verify policy exists
    iam_client = boto3.client('iam')
    policy = iam_client.get_policy(PolicyArn=f"arn:aws:iam::123456789012:policy/{policy_name}")
    assert policy['Policy']['PolicyName'] == policy_name

@mock_iam
def test_attach_policy_to_role(mock_aws_credentials, sample_kms_key):
    """Test attaching policy to role."""
    user_text = 'testuser'
    
    # Create role and policy
    role_name = create_iam_role(user_text)
    policy_name = create_kms_policy(user_text, sample_kms_key)
    
    # Attach policy to role
    attach_policy_to_role(role_name, policy_name)
    
    # Verify attachment
    iam_client = boto3.client('iam')
    attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)
    assert any(policy['PolicyName'] == policy_name for policy in attached_policies['AttachedPolicies'])

@mock_iam
def test_duplicate_role_name(mock_aws_credentials):
    """Test handling duplicate role name."""
    user_text = 'testuser'
    
    # Create first role
    create_iam_role(user_text)
    
    # Attempt to create duplicate
    with pytest.raises(Exception):
        create_iam_role(user_text)

@mock_iam
def test_duplicate_policy_name(mock_aws_credentials, sample_kms_key):
    """Test handling duplicate policy name."""
    user_text = 'testuser'
    
    # Create first policy
    create_kms_policy(user_text, sample_kms_key)
    
    # Attempt to create duplicate
    with pytest.raises(Exception):
        create_kms_policy(user_text, sample_kms_key)

@mock_iam
def test_invalid_kms_key_arn(mock_aws_credentials):
    """Test handling invalid KMS key ARN."""
    user_text = 'testuser'
    invalid_kms_arn = 'arn:aws:kms:region:account:key/invalid'
    
    with pytest.raises(Exception):
        create_kms_policy(user_text, invalid_kms_arn) 