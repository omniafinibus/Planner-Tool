from planner import get_schedule, schedule_tasks  # Import function to load calendar files.
from tasks import create_tasks_from_todo, create_ics_text  # Import function to create tasks from a todo file.
import os  # Import the os module for file and directory manipulation.
import datetime as dt

# Define which file to use for tasks
resources = '/' + os.path.join('home', 'arjan', 'Nextcloud', 'Programming', 'Projects_Python', 'Planner-Tool', 'resources')
testFile = os.path.join(resources, 'Tasks.csv')  # Path to the tasks file.

# Build all tasks from the todo file
lTasks = create_tasks_from_todo(testFile)

# Retrieve and filter schedule events
calendar = get_schedule([
        os.path.join(resources, "parallelization-compilers-and-platforms-2025-02-09.ics"),
    ]
)
today = dt.datetime.today()
lTaskSchedule = schedule_tasks(calendar, lTasks, dt.time(10, 00), dt.time(20,00), False, True)

# Remove all times from start and due dates
for task in lTaskSchedule:
    task.dueDate = task.dueDate.date()
    task.startDate = task.startDate.date()

f = open(os.path.join(resources, "tasks.ics"), "w")
f.write(create_ics_text(lTaskSchedule))
f.close()

print("Done")
