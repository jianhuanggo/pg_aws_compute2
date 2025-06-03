"""
Example script demonstrating how to use the IAM role and policy creation functionality.

This script shows:
1. How to create an IAM role
2. How to create a KMS policy
3. How to attach the policy to the role
"""

from create_iam import create_iam_role, create_kms_policy, attach_policy_to_role
from create_kms import create_kms_key

def create_role_and_policy():
    """Create an IAM role and policy for KMS access."""
    print("\nCreating IAM role and policy...")
    try:
        # First create a KMS key
        print("Creating KMS key...")
        key_details = create_kms_key(
            key_alias='example-key',
            description='Example KMS key for IAM policy'
        )
        print(f"Created KMS key: {key_details['KeyArn']}")
        
        # Create IAM role
        user_text = 'example'
        role_name = create_iam_role(user_text)
        print(f"Created IAM role: {role_name}")
        
        # Create KMS policy
        policy_name = create_kms_policy(user_text, key_details['KeyArn'])
        print(f"Created IAM policy: {policy_name}")
        
        # Attach policy to role
        attach_policy_to_role(role_name, policy_name)
        print(f"Attached policy {policy_name} to role {role_name}")
        
        return {
            'role_name': role_name,
            'policy_name': policy_name,
            'kms_key_arn': key_details['KeyArn']
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def main():
    """Main function demonstrating IAM operations."""
    print("IAM Role and Policy Creation Example")
    print("=" * 50)
    
    result = create_role_and_policy()
    
    if result:
        print("\nSummary of created resources:")
        print("-" * 50)
        print(f"IAM Role: {result['role_name']}")
        print(f"IAM Policy: {result['policy_name']}")
        print(f"KMS Key ARN: {result['kms_key_arn']}")

if __name__ == "__main__":
    main() 