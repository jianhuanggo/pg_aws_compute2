import pytest
import boto3
import json
import os
import sys
from moto import mock_ec2, mock_secretsmanager

# Add parent directory to path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from create_key_pair import (
    create_key_pair,
    save_key_pair_to_file,
    save_to_secrets_manager,
    process_usernames
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
def sample_usernames():
    """Sample usernames for testing."""
    return ['testuser1', 'testuser2']

@pytest.fixture
def username_json(tmp_path):
    """Create a temporary username.json file."""
    data = {'usernames': ['testuser1', 'testuser2']}
    file_path = tmp_path / "username.json"
    with open(file_path, 'w') as f:
        json.dump(data, f)
    return str(file_path)

@mock_ec2
def test_create_key_pair(mock_aws_credentials):
    """Test creating a key pair."""
    username = 'testuser'
    key_pair = create_key_pair(username)
    
    assert key_pair is not None
    assert 'KeyName' in key_pair
    assert key_pair['KeyName'] == f"{username}-key"
    assert 'KeyMaterial' in key_pair
    assert 'KeyPairId' in key_pair

def test_save_key_pair_to_file(tmp_path):
    """Test saving key pair to file."""
    private_key = "test-private-key"
    public_key = "test-public-key"
    username = "testuser"
    
    private_path, public_path = save_key_pair_to_file(private_key, public_key, username, str(tmp_path))
    
    assert os.path.exists(private_path)
    assert os.path.exists(public_path)
    
    with open(private_path, 'r') as f:
        assert f.read() == private_key
    
    with open(public_path, 'r') as f:
        assert f.read() == public_key

@mock_secretsmanager
def test_save_to_secrets_manager(mock_aws_credentials):
    """Test saving keys to Secrets Manager."""
    username = 'testuser'
    private_key = 'test-private-key'
    public_key = 'test-public-key'
    
    secret_arn = save_to_secrets_manager(username, private_key, public_key)
    
    assert secret_arn is not None
    
    # Verify the secret was created
    secrets_client = boto3.client('secretsmanager')
    secret = secrets_client.get_secret_value(SecretId=secret_arn)
    secret_value = json.loads(secret['SecretString'])
    
    assert secret_value['private_key'] == private_key
    assert secret_value['public_key'] == public_key

@mock_ec2
@mock_secretsmanager
def test_process_usernames(mock_aws_credentials, username_json, tmp_path):
    """Test processing multiple usernames."""
    results = process_usernames(username_json, str(tmp_path))
    
    assert len(results) == 2
    for result in results:
        assert 'username' in result
        assert 'private_key_path' in result
        assert 'public_key_path' in result
        assert 'secret_arn' in result
        assert os.path.exists(result['private_key_path'])
        assert os.path.exists(result['public_key_path'])

def test_invalid_username_json(tmp_path):
    """Test handling invalid username.json file."""
    invalid_json = tmp_path / "invalid.json"
    with open(invalid_json, 'w') as f:
        f.write("invalid json content")
    
    with pytest.raises(Exception):
        process_usernames(str(invalid_json), str(tmp_path))

@mock_ec2
def test_duplicate_key_pair(mock_aws_credentials):
    """Test handling duplicate key pair creation."""
    username = 'testuser'
    
    # Create first key pair
    create_key_pair(username)
    
    # Attempt to create duplicate
    with pytest.raises(Exception):
        create_key_pair(username) 