"""
Example script demonstrating how to use the Auto Scaling Group management functionality.

This script shows:
1. How to create an Auto Scaling Group with default configuration
2. How to delete an Auto Scaling Group
"""

from create_asg import create_asg, delete_asg

def create_and_delete_asg():
    """Create and then delete an Auto Scaling Group."""
    print("\nCreating Auto Scaling Group...")
    try:
        # Example parameters
        user_text = 'example'
        launch_template_id = 'lt-12345678'  # Replace with your launch template ID
        
        # Create ASG
        asg_name = create_asg(user_text, launch_template_id)
        print(f"Created Auto Scaling Group: {asg_name}")
        
        # Wait a moment to ensure the ASG is fully created
        import time
        time.sleep(2)
        
        # Delete the ASG
        print("\nDeleting Auto Scaling Group...")
        delete_asg(asg_name)
        print(f"Deleted Auto Scaling Group: {asg_name}")
        
        return asg_name
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def main():
    """Main function demonstrating Auto Scaling Group operations."""
    print("Auto Scaling Group Management Example")
    print("=" * 50)
    
    asg_name = create_and_delete_asg()
    
    if asg_name:
        print("\nSuccessfully completed Auto Scaling Group lifecycle:")
        print("-" * 50)
        print(f"Created and deleted Auto Scaling Group: {asg_name}")

if __name__ == "__main__":
    main() 