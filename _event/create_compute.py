
from create_kms import create_kms_key, get_kms_key_details
import os

def create_basic_key(key_alias: str, description: str):
    """Create a basic KMS key."""

    print("\nCreating basic KMS key...")
    try:
        key_details = create_kms_key(
            key_alias=key_alias,
            description=description
        )
        print("Successfully created basic key:")
        print(f"Key ID: {key_details['KeyId']}")
        print(f"Key ARN: {key_details['KeyArn']}")
        print(f"Alias: {key_details['AliasName']}")
    except Exception as e:
        print(f"Error creating basic key: {str(e)}")


from create_key_pair import process_usernames, delete_credential


def create_key_pair_file(profile_name: str, user_context: str):

    output_dir = "ec2_keys"
    os.makedirs(output_dir, exist_ok=True)
    
    # Process the usernames and create key pairs
    try:
        results = process_usernames(
            profile_name=profile_name,
            username_json_path="username.json",
            output_dir=output_dir,
            user_context=user_context
        )
        
        # Print results
        print("\nKey pair creation results:")
        print("-" * 50)
        for result in results:
            print(f"\nUsername: {result['username']}")
            print(f"Private key path: {result['private_key_path']}")
            print(f"Public key path: {result['public_key_path']}")
            print(f"Secret ARN: {result['secret_arn']}")
        
        print(f"\nSuccessfully processed {len(results)} users")
        
    except Exception as e:
        print(f"Error: {str(e)}")


from create_iam import create_iam_role, create_kms_policy, attach_policy_to_role
from create_kms import create_kms_key

def create_role_and_policy(kms_key_arn: str):
    """Create an IAM role and policy for KMS access."""
    print("\nCreating IAM role and policy...")
    try:
        
        # Create IAM role
        user_text = 'fusion-dev'
        role_name = create_iam_role(user_text)
        print(f"Created IAM role: {role_name}")
        
        #Create KMS policy
        policy_name = create_kms_policy(user_text, kms_key_arn)
        print(f"Created IAM policy: {policy_name}")
        
        # Attach policy to role
        attach_policy_to_role(role_name, policy_name)
        print(f"Attached policy {policy_name} to role {role_name}")
        
        return {
            'role_name': role_name,
            'policy_name': policy_name,
            'kms_key_arn': kms_key_arn
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

from create_security_group import create_security_group, delete_security_group

def create_and_delete_security_group():
    """Create and then delete a security group."""
    print("\nCreating security group...")
    try:
        # Create security group
        user_text = 'fusion-dev'
        vpc_id = 'vpc-0c3568c316ca169c4'  # Replace with your VPC ID
        
        group_id = create_security_group(user_text, vpc_id)
        print(f"Created security group: {group_id}")
        
        # Wait a moment to ensure the group is fully created
        import time
        time.sleep(2)
        
        # Delete the security group
        print("\nDeleting security group...")
        # delete_security_group(group_id)
        # print(f"Deleted security group: {group_id}")
        
        return group_id
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

from create_launch_template import create_launch_template, delete_launch_template


def create_and_delete_launch_template():
    """Create and then delete a launch template."""
    print("\nCreating launch template...")
    try:
        # Example parameters
        user_text = "fusion-app-dev"
        security_group_id = 'sg-04370007e7f093acb'  # Replace with your security group ID
        key_name = 'kp-fusion-dev-jianhuanggo'  # Replace with your key pair name
        iam_role_arn = 'arn:aws:iam::717435123117:role/iam-role-fusion-dev-l3qpmt'  # Replace with your IAM role ARN
        kms_key_id = 'arn:aws:kms:us-east-1:717435123117:key/4797d75f-a543-41c0-9de8-5523827637e4'  # Replace with your KMS key ID
        
        # Create example user data file
        user_data_file = 'user_data.sh'
        with open(user_data_file, 'w') as f:
            f.write('''#!/bin/bash
# Example user data script
echo "Starting instance configuration..."
yum update -y
yum install -y httpd
systemctl start httpd
systemctl enable httpd
echo "Instance configuration complete!"
''')
        
        try:
            # Create launch template
            template_id = create_launch_template(
                user_text=user_text,
                security_group_id=security_group_id,
                key_name=key_name,
                iam_role_arn=iam_role_arn,
                user_data_file=user_data_file,
                kms_key_id=kms_key_id
            )
            print(f"Created launch template: {template_id}")
            
            # Wait a moment to ensure the template is fully created
            import time
            time.sleep(2)
            
            # Delete the launch template
            # print("\nDeleting launch template...")
            # delete_launch_template(template_id)
            # print(f"Deleted launch template: {template_id}")
            
            return template_id
            
        finally:
            # Clean up user data file
            import os
            if os.path.exists(user_data_file):
                os.remove(user_data_file)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None



def create_compute():
    """
    Create a compute instance
    """
    #create_basic_key(key_alias='aws_ec2_ebsdefault', description='Example KMS key for ebs encryption')
    create_key_pair_file("tag_fusion", user_context="app-fusion-prod")
    # create_role_and_policy("arn:aws:kms:us-east-1:717435123117:key/4797d75f-a543-41c0-9de8-5523827637e4")

    #create_and_delete_security_group()
    # create_and_delete_launch_template()


def delete_compute():
    """
    Delete a compute instance
    """

    delete_credential(profile_name="tag_fusion", username_json_path="username.json", user_context="app-fusion-dev")
