"""
Task and Job Management System

This module provides functionality for managing tasks, jobs, and their dependencies
in a DAG (Directed Acyclic Graph) structure.
"""

from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import uuid

@dataclass
class Task:
    """Represents a task entity in the system.
    
    Attributes:
        task_id: Unique identifier for the task
        task_name: Name of the task
        task_description: Detailed description of the task
        task_context: Additional context information for the task
        system_created_at: Timestamp when the task was created
        system_updated_at: Timestamp when the task was last updated
    """
    task_id: str
    task_name: str
    task_description: str
    task_context: Dict
    system_created_at: datetime
    system_updated_at: datetime

@dataclass
class Job:
    """Represents a job instance of a task.
    
    Attributes:
        job_id: Unique identifier for the job
        task_id: Reference to the associated task
        job_name: Name of the job (copied from task)
        job_description: Description of the job (copied from task)
        job_context: Context information (copied from task)
        job_start_ts: Timestamp when the job started
        job_end_ts: Timestamp when the job ended
        system_created_at: Timestamp when the job was created
        system_updated_at: Timestamp when the job was last updated
    """
    job_id: str
    task_id: str
    job_name: str
    job_description: str
    job_context: Dict
    job_start_ts: datetime
    job_end_ts: Optional[datetime]
    system_created_at: datetime
    system_updated_at: datetime

@dataclass
class TaskDependency:
    """Represents a dependency relationship between tasks.
    
    Attributes:
        task_dependency_id: Unique identifier for the dependency
        task_id: ID of the dependent task
        task_parent_id: ID of the parent task
        system_created_at: Timestamp when the dependency was created
        system_updated_at: Timestamp when the dependency was last updated
    """
    task_dependency_id: str
    task_id: str
    task_parent_id: str
    system_created_at: datetime
    system_updated_at: datetime

class TaskManager:
    """Manages tasks, jobs, and their dependencies."""
    
    def __init__(self):
        """Initialize the TaskManager with empty storage."""
        self.tasks: Dict[str, Task] = {}
        self.jobs: Dict[str, Job] = {}
        self.dependencies: Dict[str, TaskDependency] = {}
        self.task_dependencies: Dict[str, Set[str]] = defaultdict(set)
        self.task_parents: Dict[str, Set[str]] = defaultdict(set)

    def create_task(self, task_name: str, task_description: str, task_context: Dict) -> Task:
        """Create a new task.
        
        Args:
            task_name: Name of the task
            task_description: Description of the task
            task_context: Context information for the task
            
        Returns:
            Task: The created task object
            
        Raises:
            ValueError: If task_name is empty
        """
        if not task_name:
            raise ValueError("Task name cannot be empty")
            
        now = datetime.utcnow()
        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            task_name=task_name,
            task_description=task_description,
            task_context=task_context,
            system_created_at=now,
            system_updated_at=now
        )
        self.tasks[task_id] = task
        return task

    def delete_task(self, task_id: str) -> None:
        """Delete a task and its associated dependencies.
        
        Args:
            task_id: ID of the task to delete
            
        Raises:
            KeyError: If task_id doesn't exist
        """
        if task_id not in self.tasks:
            raise KeyError(f"Task {task_id} not found")
            
        # Remove all dependencies where this task is involved
        dependencies_to_remove = [
            dep_id for dep_id, dep in self.dependencies.items()
            if dep.task_id == task_id or dep.task_parent_id == task_id
        ]
        for dep_id in dependencies_to_remove:
            del self.dependencies[dep_id]
            
        # Clean up dependency mappings
        if task_id in self.task_dependencies:
            del self.task_dependencies[task_id]
        if task_id in self.task_parents:
            del self.task_parents[task_id]
            
        # Remove the task
        del self.tasks[task_id]

    def update_task(self, task_id: str, task_name: Optional[str] = None,
                   task_description: Optional[str] = None,
                   task_context: Optional[Dict] = None) -> Task:
        """Update an existing task.
        
        Args:
            task_id: ID of the task to update
            task_name: New task name (optional)
            task_description: New task description (optional)
            task_context: New task context (optional)
            
        Returns:
            Task: The updated task object
            
        Raises:
            KeyError: If task_id doesn't exist
        """
        if task_id not in self.tasks:
            raise KeyError(f"Task {task_id} not found")
            
        task = self.tasks[task_id]
        if task_name is not None:
            task.task_name = task_name
        if task_description is not None:
            task.task_description = task_description
        if task_context is not None:
            task.task_context = task_context
            
        task.system_updated_at = datetime.utcnow()
        return task

    def create_task_dependency(self, task_id: str, task_parent_id: str) -> TaskDependency:
        """Create a dependency between two tasks.
        
        Args:
            task_id: ID of the dependent task
            task_parent_id: ID of the parent task
            
        Returns:
            TaskDependency: The created dependency object
            
        Raises:
            KeyError: If either task_id or task_parent_id doesn't exist
            ValueError: If the dependency would create a cycle
        """
        if task_id not in self.tasks or task_parent_id not in self.tasks:
            raise KeyError("Both tasks must exist")
            
        # Check for cycles
        if self._would_create_cycle(task_id, task_parent_id):
            raise ValueError("Creating this dependency would create a cycle")
            
        now = datetime.utcnow()
        dep_id = str(uuid.uuid4())
        dependency = TaskDependency(
            task_dependency_id=dep_id,
            task_id=task_id,
            task_parent_id=task_parent_id,
            system_created_at=now,
            system_updated_at=now
        )
        
        self.dependencies[dep_id] = dependency
        self.task_dependencies[task_parent_id].add(task_id)
        self.task_parents[task_id].add(task_parent_id)
        
        return dependency

    def remove_task_dependency(self, task_id: str, task_parent_id: str) -> None:
        """Remove a dependency between two tasks.
        
        Args:
            task_id: ID of the dependent task
            task_parent_id: ID of the parent task
            
        Raises:
            KeyError: If the dependency doesn't exist
        """
        dep_id = None
        for d_id, dep in self.dependencies.items():
            if dep.task_id == task_id and dep.task_parent_id == task_parent_id:
                dep_id = d_id
                break
                
        if dep_id is None:
            raise KeyError("Dependency not found")
            
        del self.dependencies[dep_id]
        self.task_dependencies[task_parent_id].discard(task_id)
        self.task_parents[task_id].discard(task_parent_id)

    def create_job_dag(self) -> List[Job]:
        """Create a DAG of jobs based on task dependencies.
        
        Returns:
            List[Job]: List of jobs in topological order
            
        Raises:
            ValueError: If there are cycles in the task dependencies
        """
        # Get topological order of tasks
        task_order = self._get_topological_order()
        
        # Create jobs for each task
        jobs: Dict[str, Job] = {}
        now = datetime.utcnow()
        
        for task_id in task_order:
            task = self.tasks[task_id]
            job_id = str(uuid.uuid4())
            job = Job(
                job_id=job_id,
                task_id=task_id,
                job_name=task.task_name,
                job_description=task.task_description,
                job_context=task.task_context,
                job_start_ts=now,
                job_end_ts=None,
                system_created_at=now,
                system_updated_at=now
            )
            jobs[task_id] = job
            
        return [jobs[task_id] for task_id in task_order]

    def delete_job_dag(self, jobs: List[Job]) -> None:
        """Delete a DAG of jobs.
        
        Args:
            jobs: List of jobs to delete
        """
        for job in jobs:
            if job.job_id in self.jobs:
                del self.jobs[job.job_id]

    def _would_create_cycle(self, task_id: str, task_parent_id: str) -> bool:
        """Check if adding a dependency would create a cycle.
        
        Args:
            task_id: ID of the dependent task
            task_parent_id: ID of the parent task
            
        Returns:
            bool: True if adding the dependency would create a cycle
        """
        visited = set()
        path = set()
        
        def dfs(current: str) -> bool:
            if current in path:
                return True
            if current in visited:
                return False
                
            visited.add(current)
            path.add(current)
            
            for child in self.task_dependencies[current]:
                if dfs(child):
                    return True
                    
            path.remove(current)
            return False
            
        # Temporarily add the new dependency
        self.task_dependencies[task_parent_id].add(task_id)
        self.task_parents[task_id].add(task_parent_id)
        
        # Check for cycles
        has_cycle = dfs(task_id)
        
        # Remove the temporary dependency
        self.task_dependencies[task_parent_id].discard(task_id)
        self.task_parents[task_id].discard(task_parent_id)
        
        return has_cycle

    def _get_topological_order(self) -> List[str]:
        """Get tasks in topological order.
        
        Returns:
            List[str]: List of task IDs in topological order
            
        Raises:
            ValueError: If there are cycles in the task dependencies
        """
        in_degree = defaultdict(int)
        for task_id in self.tasks:
            in_degree[task_id] = len(self.task_parents[task_id])
            
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            task_id = queue.popleft()
            result.append(task_id)
            
            for child in self.task_dependencies[task_id]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
                    
        if len(result) != len(self.tasks):
            raise ValueError("Task dependencies contain cycles")
            
        return result