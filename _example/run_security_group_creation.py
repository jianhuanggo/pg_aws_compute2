"""
Example script demonstrating how to use the security group management functionality.

This script shows:
1. How to create a security group with default rules
2. How to delete a security group
"""

from create_security_group import create_security_group, delete_security_group

def create_and_delete_security_group():
    """Create and then delete a security group."""
    print("\nCreating security group...")
    try:
        # Create security group
        user_text = 'example'
        vpc_id = 'vpc-12345678'  # Replace with your VPC ID
        
        group_id = create_security_group(user_text, vpc_id)
        print(f"Created security group: {group_id}")
        
        # Wait a moment to ensure the group is fully created
        import time
        time.sleep(2)
        
        # Delete the security group
        print("\nDeleting security group...")
        delete_security_group(group_id)
        print(f"Deleted security group: {group_id}")
        
        return group_id
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def main():
    """Main function demonstrating security group operations."""
    print("Security Group Management Example")
    print("=" * 50)
    
    group_id = create_and_delete_security_group()
    
    if group_id:
        print("\nSuccessfully completed security group lifecycle:")
        print("-" * 50)
        print(f"Created and deleted security group: {group_id}")

if __name__ == "__main__":
    main() 