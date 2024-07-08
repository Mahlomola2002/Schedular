from collections import defaultdict, deque
import heapq

class JobScheduler:
    
    def __init__(self):
        self.blocked_intervals = defaultdict(list)
        self.workstation_capacities = {}
        self.workstation_queues = defaultdict(list)
    
        
    def topological_sort(self, dependencies):
        def dfs(job):
            if job in visiting:
                raise Exception("A cycle was detected in the graph")
            if job not in visited:
                visiting.add(job)
                for neighbor in graph[job]:
                    dfs(neighbor)
                visiting.remove(job)
                visited.add(job)
                stack.append(job)

        # Initialize graph
        graph = defaultdict(list)
        all_jobs = set(dependencies.keys())
        
        for job, deps in dependencies.items():
            all_jobs.update(deps)
            for dep in deps:
                graph[dep].append(job)

        visited = set()
        visiting = set()
        stack = []

        # Perform DFS from all jobs
        for job in all_jobs:
            if job not in visited:
                dfs(job)

        return stack[::-1]  # Return reversed stack as the topological sort order

    
    

    def time_range(self, start, end, step):
        while start < end:
            yield start
            start += step

    def get_available_capacity(self, workstation, time):
        full_capacity = self.workstation_capacities[workstation]
        
        for start, end, blocked_capacity in self.blocked_intervals[workstation]:
            if start <= time < end:
                return max(0, full_capacity - blocked_capacity)
        
        return full_capacity


    def find_next_available_slot(self, workstation, duration, start_time, job):
        queue = self.workstation_queues[workstation]
        
        while True:
            current_time = start_time
            end_time = current_time + duration
            
            # Check if the entire job duration fits within available capacity
            is_slot_available = True
            max_concurrent_jobs = 0
            for t in self.time_range(current_time, end_time, 0.25):  # Check every 15 minutes
                available_capacity = self.get_available_capacity(workstation, t)
                
                # Count jobs that overlap with this time slot
                concurrent_jobs = sum(1 for job_end, _ in queue if job_end > t)
                
                
                if concurrent_jobs >= available_capacity:
                    is_slot_available = False
                    break
                
                max_concurrent_jobs = max(max_concurrent_jobs, concurrent_jobs)
            
            if is_slot_available and max_concurrent_jobs < self.workstation_capacities[workstation]:
                heapq.heappush(queue, (end_time, job))
                return current_time
            
            # If no slot is available, find the next end time in the queue or blocked interval
            next_times = [job_end for job_end, _ in queue if job_end > current_time]
            for start, end, _ in self.blocked_intervals[workstation]:
                if start > current_time:
                    next_times.append(start)
                if end > current_time:
                    next_times.append(end)
            
            if next_times:
                start_time = min(next_times)
            else:
                start_time += 0.25  # If no valid times, increment by 15 minutes
                
    def schedule_jobs_with_overlap(self, sorted_jobs, node_details, blocked_times, dependencies, strtTime):
        schedule = defaultdict(list)
        job_start_end_times = {}

        # Convert blocked times to intervals and initialize workstation capacities
        for workstation, blocks in blocked_times.items():
            for bt in blocks:
                start_time = float(bt['blocked_start_time'])
                end_time = float(bt['blocked_end_time'])
                blocked_capacity = bt['blocked_capacity']
                self.blocked_intervals[workstation].append((start_time, end_time, blocked_capacity))
            self.workstation_capacities[workstation] = next(float(item['capacity']) for item in node_details if item['workstation'] == workstation)
            self.workstation_queues[workstation] = []  # Initialize empty queue for each workstation

        global_start_time = float(strtTime)

        # Sort jobs based on their dependencies
        def get_dependency_depth(job):
            if job not in dependencies:
                return 0
            return 1 + max(get_dependency_depth(dep) for dep in dependencies[job])

        sorted_jobs.sort(key=get_dependency_depth, reverse=False)
        

        for job in sorted_jobs:
            node_detail = next(item for item in node_details if item['node'] == job)
            workstation = node_detail['workstation']
            duration = float(node_detail['time_hours'])

            # Ensure dependencies are met
            dependencies_met_time = global_start_time
            if job in dependencies:
                for dep in dependencies[job]:
                    if dep in job_start_end_times:
                        dependencies_met_time = max(dependencies_met_time, job_start_end_times[dep]['end_time'])

            # Find next available time slot after meeting dependencies
            start_time = self.find_next_available_slot(workstation, duration, dependencies_met_time, job)
            end_time = start_time + duration

            # Schedule the job
            schedule[workstation].append({'start_time': start_time, 'end_time': end_time, 'job': job})
            job_start_end_times[job] = {'workstation': workstation, 'start_time': start_time, 'end_time': end_time}

        return job_start_end_times

    def reverse_dependencies(self, dependencies):
        reversed_dependencies = defaultdict(list)
        for child, parents in dependencies.items():
            for parent in parents:
                reversed_dependencies[parent].append(child)
        return reversed_dependencies

    def runAll(self, dependencies, node_details, blocked_times, tim):
        reversed = self.reverse_dependencies(dependencies)
        
        sorted_jobs = self.topological_sort(reversed)
        print(sorted_jobs)
        
       
        scheduled = self.schedule_jobs_with_overlap(sorted_jobs, node_details, blocked_times, reversed, tim)
        
        # Sort the jobs by start time for display
        sorted_schedule = sorted(scheduled.items(), key=lambda x: x[1]['start_time'])
        
        for workstation, jobs in sorted_schedule:
            print(f"{workstation}:")
            print(jobs)

# Example usage:

dependencies = {
    "Initial Clean": [],
    "Initial Make": [],
    "Make F": ["Clean F", "Initial Clean", "Initial Make"],
    "Clean F": ["Initial Clean", "Initial Make"],
    "Clean G": ["Initial Clean", "Initial Make"],
    "Make G": ["Clean G", "Initial Clean", "Initial Make"],
    "Make H": ["Clean H", "Initial Clean", "Initial Make", "Make F", "Clean F"]
}


node_details = [
    {"node": "Initial Clean", "workstation": "Workstation 1", "time_hours": 2, "capacity": 2},
    {"node": "Initial Make", "workstation": "Workstation 2", "time_hours": 2, "capacity": 2},
    {"node": "Clean F", "workstation": "Workstation 1", "time_hours": 1, "capacity": 2},
    {"node": "Make F", "workstation": "Workstation 2", "time_hours": 1, "capacity": 2},
    {"node": "Clean G", "workstation": "Workstation 1", "time_hours": 1, "capacity": 2},
    {"node": "Make G", "workstation": "Workstation 2", "time_hours": 3, "capacity": 2},
    {"node": "Clean H", "workstation": "Workstation 1", "time_hours": 1, "capacity": 2},
    {"node": "Make H", "workstation": "Workstation 2", "time_hours": 2, "capacity": 2}
]

blocked_times = {
    "Workstation 1": [
        {"blocked_start_time": 8.0, "blocked_end_time": 9.0, "blocked_capacity": 1},
        {"blocked_start_time": 11.0, "blocked_end_time": 12.0, "blocked_capacity": 1},
        {"blocked_start_time": 16.0, "blocked_end_time": 17.0, "blocked_capacity": 2},
        {"blocked_start_time": 18.0, "blocked_end_time": 20.0, "blocked_capacity": 2},
        {"blocked_start_time": 22.0, "blocked_end_time": 23.0, "blocked_capacity": 1}
    ],
    "Workstation 2": [
        {"blocked_start_time": 8.0, "blocked_end_time": 9.0, "blocked_capacity": 1},
        {"blocked_start_time": 12.0, "blocked_end_time": 13.0, "blocked_capacity": 1},
        {"blocked_start_time": 14.0, "blocked_end_time": 15.0, "blocked_capacity": 1},
        {"blocked_start_time": 16.0, "blocked_end_time": 17.0, "blocked_capacity": 2},
        {"blocked_start_time": 18.0, "blocked_end_time": 20.0, "blocked_capacity": 2}
    ]
}



schedule = JobScheduler()

tim = float(input("Enter the start time: "))

schedule.runAll(dependencies, node_details, blocked_times, tim)






