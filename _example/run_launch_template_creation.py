"""
Example script demonstrating how to use the launch template management functionality.

This script shows:
1. How to create a launch template with default configuration
2. How to delete a launch template
"""

from create_launch_template import create_launch_template, delete_launch_template

def create_and_delete_launch_template():
    """Create and then delete a launch template."""
    print("\nCreating launch template...")
    try:
        # Example parameters
        user_text = 'example'
        security_group_id = 'sg-12345678'  # Replace with your security group ID
        key_name = 'example-key'  # Replace with your key pair name
        iam_role_arn = 'arn:aws:iam::123456789012:role/example-role'  # Replace with your IAM role ARN
        kms_key_id = 'arn:aws:kms:us-east-1:123456789012:key/example-key'  # Replace with your KMS key ID
        
        # Create example user data file
        user_data_file = 'example_user_data.sh'
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
            print("\nDeleting launch template...")
            delete_launch_template(template_id)
            print(f"Deleted launch template: {template_id}")
            
            return template_id
            
        finally:
            # Clean up user data file
            import os
            if os.path.exists(user_data_file):
                os.remove(user_data_file)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def main():
    """Main function demonstrating launch template operations."""
    print("Launch Template Management Example")
    print("=" * 50)
    
    template_id = create_and_delete_launch_template()
    
    if template_id:
        print("\nSuccessfully completed launch template lifecycle:")
        print("-" * 50)
        print(f"Created and deleted launch template: {template_id}")

if __name__ == "__main__":
    main() 