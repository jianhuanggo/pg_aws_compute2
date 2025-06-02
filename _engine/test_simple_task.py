"""
Unit tests for the Task and Job Management System.
"""

import unittest
from datetime import datetime
from typing import Dict
from _engine._simple_task import TaskManager, Task, Job, TaskDependency

class TestTaskManager(unittest.TestCase):
    """Test cases for TaskManager class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.manager = TaskManager()
        self.test_context: Dict = {"test_key": "test_value"}
        
    def test_create_task(self):
        """Test task creation functionality."""
        task = self.manager.create_task("Test Task", "Test Description", self.test_context)
        
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_name, "Test Task")
        self.assertEqual(task.task_description, "Test Description")
        self.assertEqual(task.task_context, self.test_context)
        self.assertIsInstance(task.system_created_at, datetime)
        self.assertIsInstance(task.system_updated_at, datetime)
        
    def test_create_task_empty_name(self):
        """Test task creation with empty name."""
        with self.assertRaises(ValueError):
            self.manager.create_task("", "Test Description", self.test_context)
            
    def test_delete_task(self):
        """Test task deletion functionality."""
        task = self.manager.create_task("Test Task", "Test Description", self.test_context)
        self.manager.delete_task(task.task_id)
        
        with self.assertRaises(KeyError):
            self.manager.delete_task(task.task_id)
            
    def test_update_task(self):
        """Test task update functionality."""
        task = self.manager.create_task("Test Task", "Test Description", self.test_context)
        new_context = {"new_key": "new_value"}
        
        updated_task = self.manager.update_task(
            task.task_id,
            task_name="Updated Task",
            task_description="Updated Description",
            task_context=new_context
        )
        
        self.assertEqual(updated_task.task_name, "Updated Task")
        self.assertEqual(updated_task.task_description, "Updated Description")
        self.assertEqual(updated_task.task_context, new_context)
        
    def test_update_task_nonexistent(self):
        """Test updating a nonexistent task."""
        with self.assertRaises(KeyError):
            self.manager.update_task("nonexistent_id", task_name="New Name")
            
    def test_create_task_dependency(self):
        """Test creating task dependencies."""
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        
        dependency = self.manager.create_task_dependency(task2.task_id, task1.task_id)
        
        self.assertIsInstance(dependency, TaskDependency)
        self.assertEqual(dependency.task_id, task2.task_id)
        self.assertEqual(dependency.task_parent_id, task1.task_id)
        
    def test_create_task_dependency_cycle(self):
        """Test creating a cyclic dependency."""
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        task3 = self.manager.create_task("Task 3", "Description 3", self.test_context)
        
        # Create dependencies: task1 -> task2 -> task3
        self.manager.create_task_dependency(task2.task_id, task1.task_id)
        self.manager.create_task_dependency(task3.task_id, task2.task_id)
        
        # Try to create a cycle: task3 -> task1
        with self.assertRaises(ValueError):
            self.manager.create_task_dependency(task1.task_id, task3.task_id)
            
    def test_remove_task_dependency(self):
        """Test removing task dependencies."""
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        
        self.manager.create_task_dependency(task2.task_id, task1.task_id)
        self.manager.remove_task_dependency(task2.task_id, task1.task_id)
        
        # Try to remove non-existent dependency
        with self.assertRaises(KeyError):
            self.manager.remove_task_dependency(task2.task_id, task1.task_id)
            
    def test_create_job_dag(self):
        """Test creating a job DAG."""
        # Create tasks
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        task3 = self.manager.create_task("Task 3", "Description 3", self.test_context)
        
        # Create dependencies: task1 -> task2 -> task3
        self.manager.create_task_dependency(task2.task_id, task1.task_id)
        self.manager.create_task_dependency(task3.task_id, task2.task_id)
        
        # Create job DAG
        jobs = self.manager.create_job_dag()
        
        self.assertEqual(len(jobs), 3)
        self.assertIsInstance(jobs[0], Job)
        self.assertEqual(jobs[0].task_id, task1.task_id)
        self.assertEqual(jobs[1].task_id, task2.task_id)
        self.assertEqual(jobs[2].task_id, task3.task_id)
        
    def test_create_job_dag_with_cycle(self):
        """Test creating a job DAG with cycles."""
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        
        # Create a cycle
        self.manager.create_task_dependency(task2.task_id, task1.task_id)
        self.manager.create_task_dependency(task1.task_id, task2.task_id)
        
        with self.assertRaises(ValueError):
            self.manager.create_job_dag()
            
    def test_delete_job_dag(self):
        """Test deleting a job DAG."""
        # Create tasks and jobs
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        
        self.manager.create_task_dependency(task2.task_id, task1.task_id)
        jobs = self.manager.create_job_dag()
        
        # Delete jobs
        self.manager.delete_job_dag(jobs)
        
        # Verify jobs are deleted
        for job in jobs:
            self.assertNotIn(job.job_id, self.manager.jobs)
            
    def test_task_dependency_cleanup(self):
        """Test cleanup of task dependencies when deleting a task."""
        task1 = self.manager.create_task("Task 1", "Description 1", self.test_context)
        task2 = self.manager.create_task("Task 2", "Description 2", self.test_context)
        
        # Create dependency
        self.manager.create_task_dependency(task2.task_id, task1.task_id)
        
        # Delete task1
        self.manager.delete_task(task1.task_id)
        
        # Verify dependencies are cleaned up
        self.assertNotIn(task1.task_id, self.manager.task_dependencies)
        self.assertNotIn(task1.task_id, self.manager.task_parents)
        
if __name__ == '__main__':
    unittest.main() 