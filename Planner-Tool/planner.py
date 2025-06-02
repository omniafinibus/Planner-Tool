from dataclasses import dataclass
from tasks import Task
import dateutil as du
from dateutil.rrule import MO,TU,WE,TH,FR,SA,SU,YEARLY,MONTHLY,WEEKLY,DAILY,HOURLY,MINUTELY,SECONDLY
import datetime as dt
import arrow
import pytz
import ics.timeline
import ics
import os
import re


_TIME_ZONE = pytz.timezone("Europe/Amsterdam")
_TODAY = dt.datetime.today().date()
_FIRST_LAST_DUE = _TODAY + dt.timedelta(days=1)
_DELTA_YEAR = 1
_LAST_YEAR = _TODAY.year + _DELTA_YEAR
_WEEKDAY = {'MO': MO,
           'TU': TU,
           'WE': WE,
           'TH': TH,
           'FR': FR,
           'SA': SA,
           'SU': SU
           }
_FREQ = {"YEARLY":YEARLY,
      "MONTHLY":MONTHLY,
      "WEEKLY":WEEKLY,
      "DAILY":DAILY,
      "HOURLY":HOURLY,
      "MINUTELY":MINUTELY,
      "SECONDLY":SECONDLY}

@dataclass
class TagTime:
  """Define a dataclass `TagTime` to store information about task-related time."""

  minutes: int
  """Total minutes allocated to a tag."""
  ratio: float
  """The proportion of time compared to the total time."""
  items: int
  """The count of items/tasks associated with the tag."""

  def add_item(self, itemTimeDelta:dt.timedelta):
    self.items += 1
    self.minutes += itemTimeDelta.seconds/60
    
  def remove_item(self, itemTimeDelta:dt.timedelta):
    self.items -= 1
    self.minutes -= itemTimeDelta.seconds/60
  
  def __repr__(self):
    """
    Returns a string representation of the time in the format 'hours:minutes'.

    Returns:
    str: A string representing the time, with hours and minutes separated by a colon.
    """
    return f"{int(self.minutes / 60):02}:{int(self.minutes % 60):02}"


@dataclass
class IcsVariable:
  """Define a dataclass `IcsVariable` to store information about variable in ics format and rrule variables."""

  icsString: str
  """Name of the variable in the ICS file"""
  varName: str
  """Name of the variable in the rrule function"""
  regEx: str
  """Regular expression used to extract information from line"""


class DueDate:
  """Represents a due date for a set of tasks associated with a specific tag."""

  time: TagTime
  """time (TagTime): All time variables related to this due date."""
  datetime: dt.datetime
  """date (dt.datetime): The date by which the tasks should be completed."""
  lTasks: list
  """lTasks (list): A list of tasks associated with the due date."""

  def __init__(self, firstTask: Task):
    self.time = TagTime(
      int(firstTask.time.seconds / 60) if firstTask.time else 0, 0.0, 1
    )
    if isinstance(firstTask.dueDate, dt.datetime):
      self.datetime = firstTask.dueDate
    else:
      raise Exception(
        f"Uncategorized task found: {firstTask.task}, add a due date to this task or move this task above a task with the same tag and with a due date."
      )
    self.lTasks = [firstTask]

  def pop(self, index):
    task = self.lTasks.pop(index)
    if task.time:
      self.time.remove_item(task.time)
    return task

  def add_task(self, task):
    self.lTasks.insert(0, task)
    if task.time:
      self.time.add_item(task.time)


def get_minutes(fromTime: dt.time, tillTime: dt.time):
  """
  Calculates the total number of minutes between two `datetime.time` objects.

  Args:
    fromTime (dt.time): The starting time.
    tillTime (dt.time): The ending time.

  Returns:
    int: The difference in minutes between `fromTime` and `tillTime`.
  """
  # Convert `fromTime` to total minutes since midnight.
  from_minutes = fromTime.hour * 60 + fromTime.minute

  # Convert `tillTime` to total minutes since midnight.
  till_minutes = tillTime.hour * 60 + tillTime.minute

  # Return the difference between `tillTime` and `fromTime` in minutes.
  return int(till_minutes - from_minutes)


def latest(firstMoment, secondMoment):
  """
  Returns the later of two moments in time.

  Args:
    firstMoment: The first time or datetime object.
    secondMoment: The second time or datetime object.

  Returns:
    The later of the two moments. If both are equal, returns `firstMoment`.
  """
  # Check if the first moment is later than or equal to the second moment.
  if firstMoment >= secondMoment:
    return firstMoment

  # Otherwise, return the second moment as it is later.
  return secondMoment


def earliest(firstMoment, secondMoment):
  """
  Returns the earlier of two moments in time.

  Args:
    firstMoment: The first time or datetime object.
    secondMoment: The second time or datetime object.

  Returns:
    The earlier of the two moments. If both are equal, returns `firstMoment`.
  """
  # Check if the first moment is earlier than or equal to the second moment.
  if firstMoment <= secondMoment:
    return firstMoment

  # Otherwise, return the second moment as it is earlier.
  return secondMoment


def get_arrow(time, date):
  """
  Converts a given time and date into an Arrow object.

  Args:
    time (datetime.time): The time to include in the Arrow object.
    date (datetime.date): The date to include in the Arrow object.

  Returns:
    Arrow: An Arrow object representing the combined date and time.
  """
  return arrow.get(dt.datetime.combine(date, time))


def get_recurring_uids(text):
  """
  Extract UIDs (unique identifiers) and RRULEs (recurrence rules) from a calendar file's text.

  Args:
    text (str): The raw text content of the calendar file.

  Returns:
    tuple: Two lists, one containing UIDs and the other containing corresponding RRULEs.
  """
  lUid = []  # Initialize a list to store UIDs.
  lRrule = []  # Initialize a list to store RRULEs.

  
  # Split the text into segments by "BEGIN" and filter those containing an "RRULE".
  for object in [item for item in text.split("BEGIN") if re.findall("RRULE:", item)]:
    for line in object.split('\n'):
      if re.findall("^RRULE:", line):
        # Extract and clean the RRULE part from the object.
        lRrule.append(re.sub(r"^RRULE:|\n$", "", line))
      elif re.findall("^UID:", line):
        # Extract and clean the UID part from the object.
        lUid.append(re.sub(r"^UID:|\n$", "", line))

  # Return the lists of UIDs and RRULEs as a tuple.
  return lUid, lRrule


def get_recurrance_dates(rrule, event: ics.Event):
  """
  Generate a list of dates based on the recurrence rule (RRULE) for a given calendar event.

  Args:
    rrule (str): The recurrence rule string extracted from the event.
    event (ics.Event): The calendar event object from which the start time is taken.

  Returns:
    list: A list of `datetime` objects representing all occurrences of the event based on the recurrence rule.
  """
  # Initialize default variables for the recurrence rule.
  dVariable = {
    "dtstart":du.parser.parse(event.begin.datetime.strftime("%Y%m%dT%H%M%SZ")),
    "wkst":MO,
    "until":du.parser.parse(f"{_LAST_YEAR}1231T235959Z")}

  # Create mappings for various RRULE components to their corresponding `dateutil.rrule` parameters.
  lVarNames = [
    # Define mappings for BYDAY and other recurrence rule components.
    IcsVariable(f"{icsName}=", rruleName, rf"{icsName}=|\s*$")
    for icsName, rruleName in [("BYDAY", "byweekday")]
    + [
      (name.upper(), name.lower())
      for name in [
        # These are standard RRULE parameters as per the iCalendar specification.
        "freq",
        "interval",
        "count",
        "until",
        "bysetpos",
        "bymonth",
        "bymonthday",
        "byyearday",
        "byeaster",
        "byweekno",
        "byhour",
        "byminute",
        "bysecond",
      ]
    ]
  ]

  # Parse each component in the RRULE string.
  for data in re.sub(r"RRULE:", "", rrule).split(
    ";"
  ):  # Remove "RRULE:" prefix and split by semicolons.
    for var in lVarNames:  # Iterate through the RRULE variable mappings.
      if var.varName == "until" and re.findall(var.icsString, data):
        # Special case for "UNTIL": Parse it as a datetime object.
        dVariable[var.varName] = du.parser.parse(re.sub(var.regEx, "", data))
      elif var.varName == "freq" and re.findall(var.icsString, data):
        # Special case for "UNTIL": Parse it as a datetime object.
        dVariable[var.varName] = _FREQ[re.sub(var.regEx, "", data)]
      elif var.varName == "byweekday" and re.findall(var.icsString, data):
        # Special case to handle the "byweekday" recurrence rule.
        # Extracts the list of weekdays from the provided data by removing the matched pattern using regex.
        dVariable[var.varName] = list()
        
        for string in list(re.sub(var.regEx, "", data).split(",")):
          # Check if the weekday string contains a negative offset (e.g., "-2MO" for "second to last Monday").
          if re.findall(r"-[0-9]", string):
            # Extract numerical values from the string (e.g., "2" from "-2MO").
            lInts = re.findall(r"[0-9]*", string)
            # Map the weekday abbreviation to the corresponding _WEEKDAY and apply the negative offset.
            dVariable[var.varName].append(_WEEKDAY[string[2:]]((-int(lInts[1]))))
          # Check for a positive offset (e.g., "2MO" for "second Monday").
          elif re.findall(r"[0-9]", string):
            # Extract numerical values from the string.
            lInts = re.findall(r"[0-9]*", string)
            # Map the weekday abbreviation to the corresponding _WEEKDAY and apply the positive offset.
            dVariable[var.varName].append(_WEEKDAY[string[1:]]((int(lInts[0]))))
          else:
            # If no offset is specified, directly map the weekday abbreviation to the corresponding _WEEKDAY.
            dVariable[var.varName].append(_WEEKDAY[string])
      elif re.findall(",", data) and re.findall(var.icsString, data):
        # For parameters with multiple values (e.g., "BYDAY=MO,WE,FR"), split by commas.
        dVariable[var.varName] = list(re.sub(var.regEx, "", data).split(","))
      elif re.findall(var.icsString, data):
        # For single-value parameters, clean and assign directly.
        dVariable[var.varName] = int(re.sub(var.regEx, "", data))

  # Generate a list of recurrence dates using `dateutil.rrule` with the parsed variables.
  return list(du.rrule.rrule(**dVariable))


def get_schedule(lDirectories):
  """
  Processes a list of file paths containing calendar data, extracts events (including recurring ones),
  and consolidates them into a single `ics.Calendar` object.

  Args:
    lDirectories (list): List of file paths to .ics calendar files.

  Returns:
    ics.Calendar: A consolidated calendar object containing all events.
  """
  # Initialize an empty calendar object to store all consolidated events.
  calendar = ics.Calendar()

  # Iterate through provided directory paths, filtering out invalid or nonexistent files.
  for directory in [dir for dir in lDirectories if os.path.isfile(dir)]:
    print(f"Opening file: {directory}")  # Log the file being processed.

    # Read the content of the .ics file.
    with open(directory) as f:
      rawData = f.read()

    # Merge events from the current file into the main calendar object.
    calendar.events = calendar.events.union(ics.Calendar(rawData).events)

    # Extract recurring event UIDs and their corresponding RRULEs from the raw data.
    lUids, lRrules = get_recurring_uids(rawData)

    newSet = set()
    # Iterate over all events currently in the calendar.
    for event in calendar.events:
      # Ensure the event's start and end times have the correct timezone (Amsterdam).
      event.begin.replace(tzinfo=_TIME_ZONE)
      event.end.replace(tzinfo=_TIME_ZONE)

      # Check if the event's UID exists in the list of recurring event UIDs.
      recurranceIndex = lUids.index(event.uid) if event.uid in lUids else -1

      # If the event is a recurring one, process its recurrences.
      if recurranceIndex != -1:
        # Generate recurrence dates using the RRULE and associated event data.
        for recurrance in get_recurrance_dates(lRrules[recurranceIndex], event):
          # Filter out recurrences beyond the specified last year (_LAST_YEAR).
          if recurrance.year <= _LAST_YEAR:
            # Clone the original event for each recurrence.
            newEvent = event.clone()

            # Update the start and end times for the new event based on the recurrence date.
            newEvent.end = arrow.get(recurrance + event.duration)
            newEvent.begin = arrow.get(recurrance)

            # Add the new event to the calendar.
            newSet.add(newEvent)
    calendar.events = calendar.events.union(newSet)
  # Return the consolidated calendar containing all events, including recurring instances.
  return calendar


def get_free_time(
  calendar: ics.Calendar,
  startDate: dt.date,
  endDate: dt.date,
  startTime: dt.time,
  stopTime: dt.time,
  skipSat: bool,
  skipSun: bool,
):
  """
  Calculate the total free time (in minutes) within a specified date and time range.

  Args:
    calendar (ics.Calendar): The calendar containing scheduled events.
    startDate (dt.date): The starting date for the free time calculation.
    endDate (dt.date): The ending date for the free time calculation.
    startTime (dt.time): The daily start time for the free time calculation.
    stopTime (dt.time): The daily stop time for the free time calculation.
    skipSat (bool): Whether to skip Saturdays.
    skipSun (bool): Whether to skip Sundays.

  Returns:
    int: Total free time (in minutes)
  """
  totalFreeTime = 0  # Initialize total free time to zero.

  # Align startTime and stopTime with the startDate.
  startTime = get_arrow(startTime, startDate)
  stopTime = get_arrow(stopTime, startDate)
  currentDate = startTime.datetime.date()  # Initialize the loop with the start date.

  # Iterate over each day within the range [startDate, endDate].
  while currentDate <= endDate:
    # Check if the current day is eligible based on skipSat and skipSun flags.
    if (
      not (skipSat and currentDate.weekday() == 5)  # Include Saturday if not skipped.
      or (not skipSun and currentDate.weekday() == 6)  # Include Sunday if not skipped.
      or currentDate.weekday() < 5  # Always include weekdays (Monday to Friday).
    ):
      lastEndTime = (
        startTime.datetime.time()
      )  # Initialize the last end time for the day.

      # Process all events occurring within the time range for the day.
      lEvents = list(calendar.timeline.included(
        arrow.get(startTime), arrow.get(stopTime)
      ))
      lEvents.append(ics.Event("start", get_arrow(dt.time(0,0,0), startTime.date()), startTime))
      lEvents.append(ics.Event("stop", stopTime, get_arrow(dt.time(23,59,59), stopTime.date())))
  
      for event in lEvents:
        # If an event overlaps with the current free time range.
        if event.begin.time() <= lastEndTime < event.end.time():
          # Move the last end time forward to the event's end or stop time, whichever is earlier.
          lastEndTime = earliest(event.end.time(), stopTime.time())

        # If there is a gap between the last end time and the event's start time.
        elif lastEndTime < event.begin.time() and lastEndTime < stopTime.time():
          # Add the gap (free time) to the total.
          totalFreeTime += get_minutes(lastEndTime, event.begin.time())
          # Update lastEndTime to the event's end or stop time, whichever is earlier.
          lastEndTime = earliest(event.end.time(), stopTime.time())

        # Add remaining free time after the last event until the stop time.
        if lastEndTime < stopTime.time():
          totalFreeTime += get_minutes(lastEndTime, stopTime.time())

    # Move to the next day, updating stopTime, startTime, and currentDate.
    stopTime += dt.timedelta(days=1)
    startTime += dt.timedelta(days=1)
    currentDate += dt.timedelta(days=1)

  # Return the total free time calculated and the event count (unchanged).
  return totalFreeTime


def sort_tasks(lTasks):
  """
  Sorts a list of tasks into a dictionary grouped by their tags, associating each tag
  with a list of due dates and related tasks. Provides feedback on task details
  like missing times or long durations.

  Args:
    lTasks (list): A list of task objects, each with attributes such as `tag`,
                  `dueDate`, `ignoreDue`, `parent`, `time`, `uid`, and `task`.

  Returns:
    dict: A dictionary where keys are task tags, and values are lists of DueDate objects.
  """
  dlTasks = dict()  # Stores tasks grouped by their tags.

  for task in reversed(lTasks):  # Iterate through tasks in reverse order.
    tag = task.tag  # Extract the tag associated with the task.

    # Check if the task has a due date and should not be ignored.
    if task.dueDate is not None:
      # Create a new list for tasks under this tag if it doesn't exist.
      if tag not in dlTasks.keys():
        dlTasks[tag] = [DueDate(task)]
      elif dlTasks[tag][0].datetime.date() == task.dueDate.date():
        # Add the task in a DueDate object at the start of the lTasks list.
        dlTasks[tag][0].add_task(task)
      else:
        # Add the task as a DueDate object at the start of the list.
        dlTasks[tag].insert(0, DueDate(task))
        
    elif tag in dlTasks.keys():
      # If the tag already exists, add the task to the most recent DueDate.
      dlTasks[tag][0].add_task(task)

  return dlTasks  # Return the dictionary of tasks grouped by tags.


def check_tasks(dlTasks):
    """
    Analyzes tasks grouped by tags to identify issues, calculate total time, and determine time ratios for each tag.

    Args:
      dlTasks (dict): A dictionary where keys are tags and values are lists of DueDate objects containing tasks.

    Returns:
      tuple:
        - totalTime (int): Total time in minutes for all tasks.
        - dRatios (dict): A dictionary with tags as keys and their respective time ratios as values.
    """
    # Identify all unique parent task IDs from the tasks in DueDate objects
    lParents = list({}.fromkeys([task.parent for lDueDates in dlTasks.values() for dueDate in lDueDates for task in dueDate.lTasks if task.parent != "" and task.parent]).keys())
        
    print(f"Check if all tasks are clearly defined:")
  
    # Analyze each task for time and chunking recommendations
    for task in [task for lDueDates in dlTasks.values() for dueDate in lDueDates for task in dueDate.lTasks]:
        # Suggest adding time if it's missing and the task isn't a parent
        if task.time == "" and task.uid not in lParents:
            print(f'\tConsider adding time to the task "{task.task}"')

        # Suggest breaking down tasks exceeding an hour
        if isinstance(task.time, dt.timedelta) and task.time.seconds > 3600:
            print(
                f'\tConsider chunking the task "{task.task}" as it takes '
                f"{int(task.time.seconds / 3600):02}:{int((task.time.seconds % 3600) / 60):02} hours."
            )

    # Calculate total time across all tasks
    totalTime = sum(
        [
            dueDate.time.minutes
            for lDueDates in dlTasks.values()
            for dueDate in lDueDates
        ]
    )

    # Calculate time ratios per tag
    dRatios = {
        tag: sum([dueDate.time.minutes for dueDate in dlTasks[tag]]) / totalTime
        for tag in dlTasks.keys()
    }

    # Display total time and equivalent ECs for each tag
    for tag in dlTasks.keys():
        totalTagTime = totalTime * dRatios[tag]
        print(
            f"Total time for {tag} is {int(totalTagTime):06} minutes, {totalTagTime / (28 * 60):.3} ECs"
        )

    return totalTime, dRatios


def assign_time(dlTasks, dueDate, tag, totalFreeTime, dRatios):
  """
  Assigns time to a given due date and calculates remaining time for subtasks.

  Args:
    dlTasks (dict): A dictionary of tasks organized by tags.
    dueDate (DueDate): The current due date object being processed.
    tag (str): The primary tag for the current due date.
    totalFreeTime (int): Total available free time in minutes.
    dRatios (dict): A dictionary of time distribution ratios for each tag.

  Returns:
    dict: Dictionary containing a TagTime object for each tag related assigned until the due date
  """
  # Calculate the ratio of this due date's time to the total free time
  dueDate.time.ratio = (dueDate.time.minutes / totalFreeTime) if totalFreeTime != 0 else 1 
  
  # Calculate remaining time after assigning to the current due date
  remTime = totalFreeTime - dueDate.time.minutes
  remTime = remTime if remTime >= 0 else 0  # Ensure remaining time is non-negative
  remRatio = 1 - dueDate.time.ratio
  
  # Dictionary to store assigned time for other subtags
  dAssignedTime = {tag: dueDate.time}
  
  # Combine any tags which share a due date
  lRemTags = []
  for subTag in [key for key in dlTasks.keys() if key != tag]:
    if dlTasks[subTag] and dlTasks[subTag][0].datetime.date() == dueDate.datetime.date():
      # Assign time to shared due date
      dAssignedTime[subTag] = dlTasks[subTag][0].time
      remTime -= dAssignedTime[subTag].minutes
      # Update the remaining ratio and due date rate
      dlTasks[subTag][0].time.ratio = dAssignedTime[subTag].minutes / totalFreeTime if totalFreeTime != 0 else 1 
      remRatio -= dAssignedTime[subTag].ratio
    else:
      # 
      lRemTags.append(subTag)

  # Assign remaining time proportionally to subtags
  for subTag in dlTasks.keys():
    if subTag not in lRemTags:
      # Calculate assigned time
      dAssignedTime[subTag].minutes += dRatios[subTag] * remTime
      dAssignedTime[subTag].ratio += dRatios[subTag] 
    else:
      dAssignedTime[subTag] = TagTime(dRatios[subTag] * remTime, dRatios[subTag], 0)  # Calculate assigned time
    
  # Print the assignments
  print(f"due:{dueDate.datetime} tot:{int(totalFreeTime):7} | focus:{tag} | " + " | ".join(sorted([f"{key}:{int(val.minutes): 6}" for key,val in dAssignedTime.items()])))

  # If there's no remaining time, warn about insufficient time
  if remTime <= 0:
    print(
      " Not enough time to make deadline, alter deadline requirements or assigned work hours"
    )
    
  return dAssignedTime


def get_target_tag(dAssignedTime, dPlannedTime, lTags):
  """
  Identifies the tag with the highest ratio of assigned time to planned time.

  Args:
    dAssignedTime (dict): A dictionary mapping tags to the assigned time.
    dPlannedTime (dict): A dictionary mapping tags to the planned time.
    lTags (list): A list of tags to evaluate.

  Returns:
    str or None: The tag with the highest ratio of assigned to planned time, 
                 or None if no tags are found.
  """
  # Return None there are no tags to choose from
  if not lTags:
    return None
  
  # Calculate the maximum ratio of assigned time to planned time across all tags
  maxLager = max([dAssignedTime[tag].minutes / dPlannedTime[tag] for tag in lTags], default=None)
  
  # Return None if no valid maximum ratio is found
  if maxLager is None:
    return None

  # Iterate through tags to find the one with the maximum ratio
  for tag in lTags:
    if dAssignedTime[tag].minutes / dPlannedTime[tag] == maxLager:
      return tag
      

def plan_day(date: dt.date, startTime: dt.time, stopTime: dt.time, 
             dAssignedTime: dict[TagTime], dPlannedTime: dict[int], 
             dlTasks: dict[list[Task]], calendar: ics.Calendar):
  """
  Plans and schedules tasks for the day based on the available time and assigned tasks.

  Args:
    date (dt.date): The date for scheduling tasks.
    startTime (dt.time): The start time of the planning period.
    stopTime (dt.time): The stop time of the planning period.
    dAssignedTime (dict[TagTime]): A dictionary of assigned time for each tag.
    dPlannedTime (dict[int]): A dictionary tracking the planned time for each tag.
    dlTasks (dict[list[Task]]): A dictionary of tasks, grouped by tags.
    calendar (ics.Calendar): The calendar containing events to consider as scheduled time.

  Returns:
    list[ics.Event]: A list of scheduled tasks with their planned start and end times.
  """
  
  # Define the beginning and end of the day (00:00 and 23:59:59)
  timeFrom = dt.time(23, 59, 59)
  dateFrom = date-dt.timedelta(days=1)
  timeTill = dt.time(0, 0, 1)
  dateTill = date+dt.timedelta(days=1)
  
  # List to hold scheduled tasks for the day
  lScheduledTasks = []
  
  # Retrieve all events for the current day from the calendar
  lTodaysEvents = list(calendar.timeline.included(get_arrow(timeFrom, dateFrom), get_arrow(timeTill, dateTill)))
  
  # Add start and stop events to represent the start and end of the planning period
  lTodaysEvents.append(ics.Event("start", get_arrow(timeTill, date), get_arrow(startTime, date)))
  lTodaysEvents.append(ics.Event("stop", get_arrow(stopTime, date), get_arrow(timeFrom, date+dt.timedelta(days=1))))
  
  # Sort events by their start time
  lTodaysEvents.sort(key=lambda x: x.begin)
  
  # Initialize the list of tags (keys of the dlTasks dictionary)
  lLocalTags = list(dlTasks.keys())
  
  # Start with the start time of the day
  lastEnd = dt.datetime.combine(date, startTime)
  dayDone = False  # Flag to indicate if the planning day is over
  
  # Main loop to schedule tasks for the day
  while not dayDone:
    task = None
    
    # Find the next task to schedule based on available time and assigned time
    while task is None:
      # Get the target tag (the one with the highest priority)
      targetTag = get_target_tag(dAssignedTime, dPlannedTime, lLocalTags)
      
      # If no valid target tag is found, return the scheduled tasks so far
      if targetTag is None:
        return lScheduledTasks

      # Check if there are tasks under the current target tag
      if dlTasks[targetTag] and dlTasks[targetTag][0].lTasks:
        # If the task has a specific duration, check if it fits within the available time
        if isinstance(dlTasks[targetTag][0].lTasks[0].time, dt.timedelta):
          task = dlTasks[targetTag][0].lTasks[0]
          duration = task.time.seconds / 60  # Convert the duration to minutes
          
          # If the task duration is larger than the remaining time, remove it from the plan
          if get_minutes(lastEnd.time(), stopTime) < duration:
            lLocalTags.remove(targetTag)
            task = None
        else:
          # Add the task to the scheduled tasks
          
          lScheduledTasks.append(dlTasks[targetTag][0].pop(0))
      # If all tasks have been assigned time, stop the scheduling process
      elif sum([dAssignedTime[tag].minutes for tag in dlTasks.keys()]) == 0:
        return lScheduledTasks
      else:
        # No task available, so set the assigned time for the target tag to 0 and remove from local tags
        if dlTasks[targetTag]:
          dlTasks[targetTag].pop(0)
        dAssignedTime[targetTag].minutes = 0
        lLocalTags.remove(targetTag)

    # Update the end time after assigning the task
    lastEnd = lTodaysEvents[0].end.datetime
    
    # Loop through the scheduled events of the day
    for event in lTodaysEvents[1:]:
      # If there's a gap between the last task and the next event, check if the task fits in the gap
      if event.begin.datetime > lastEnd:
        freeTimeDuration = get_minutes(lastEnd.time(), event.begin.time())  # Calculate the free time in minutes
        finishedAtHour = lastEnd.time().hour + int((lastEnd.time().minute + duration) / 60)  # Calculate the finish hour
        finishedAtMinute = int((lastEnd.time().minute + duration) % 60)  # Calculate the finish minute
        
        # If there is enough free time for the task, schedule it
        if freeTimeDuration >= duration:
          task.startDate = lastEnd  # Calculate the task start datetime
          task.dueDate = task.startDate + task.time  # Calculate the task due datetime
          dPlannedTime[targetTag] += duration  # Update the planned time for the tag

          # Add the task to the scheduled list
          lScheduledTasks.append(dlTasks[targetTag][0].pop(0))
          # Create an event for the scheduled task and add it to the list of events
          lTodaysEvents.append(ics.Event(task.task, task.startDate, task.dueDate))
          # Re-sort the events based on their start times
          lTodaysEvents.sort(key=lambda x: x.begin.time())
          break
        # If the task cannot be scheduled within the free time, check if it fits in the day
        elif finishedAtHour > 23 or dt.time(finishedAtHour, finishedAtMinute) > stopTime:
          # If the task cannot fit, mark the day as done
          dayDone = True
          break
        else:
          # If the task can't fit, move the last end time to the end of the current event
          lastEnd = event.end.datetime
      else:
        # If no gap is found, just update the last end time to the end of the event
        lastEnd = event.end.datetime
        
    # If the last end time is beyond the stop time, stop the scheduling process
    if lastEnd >= get_arrow(stopTime, date):
      return lScheduledTasks
  
  # Return the list of scheduled tasks
  return lScheduledTasks


def add_dates_to_parents(lTasks: list[Task]):
  
  lParents = [key for key in dict().fromkeys([task.parent for task in lTasks if task.parent != ''])]
  dDueDates = {parent: dt.datetime.today() for parent in lParents}
  dStartDates = {parent: dt.datetime.today() + dt.timedelta(weeks=52*_DELTA_YEAR) for parent in lParents}
  
  for parent in lParents:
    dDueDates[parent] = dDueDates[parent].replace(tzinfo=_TIME_ZONE)
    dStartDates[parent] = dStartDates[parent].replace(tzinfo=_TIME_ZONE)
  
  for task in lTasks:
    if task.parent != '':
      if isinstance(task.startDate, dt.datetime):
        task.dueDate = task.dueDate.replace(tzinfo=_TIME_ZONE)
        dDueDates[task.parent] = max(dDueDates[task.parent], task.dueDate)
      if isinstance(task.startDate, dt.datetime):
        task.startDate = task.startDate.replace(tzinfo=_TIME_ZONE)
        dStartDates[task.parent] = min(dStartDates[task.parent], task.startDate)
      
  for task in lTasks:
    if task.uid in lParents:
      task.dueDate = dDueDates[task.uid]
      task.startDate = dStartDates[task.uid]
      

def schedule_tasks(
  calendar: ics.Calendar,
  lTasks: list[Task],
  startTime: dt.time,
  stopTime: dt.time,
  skipSat: bool,
  skipSun: bool,
):
  """
  Schedules tasks by first sorting them and calculating available time, 
  then assigning time slots based on available time and task priorities.

  Args:
    calendar (ics.Calendar): The calendar containing events to consider for scheduling.
    lTasks (list[Task]): A list of tasks to be scheduled.
    startTime (dt.time): The start time for task scheduling.
    stopTime (dt.time): The stop time for task scheduling.
    skipSat (bool): Whether or not to skip Saturdays.
    skipSun (bool): Whether or not to skip Sundays.

  Returns:
    list[ics.Event]: A list of scheduled tasks, represented as events.
  """
  
  # Sort tasks based on tags and calculate total time and task ratios
  dlTasks = sort_tasks(lTasks)

  # Sort the tasks by their due date to plan them in the correct order
  tlSortedDueDates = sorted(
    [(dueDate, tag) for tag, lDueDates in dlTasks.items() for dueDate in lDueDates],
    key=lambda x: x[0].datetime,  # Sort by the due date of the task
  )

  # Check tasks and calculate total time and ratios for each tag
  totalTime, dRatios = check_tasks(dlTasks)

  # Initialize an empty list to store scheduled tasks
  lScheduledTasks = []
  
  # Set the initial lastDueDate to today's date
  lastDueDate = _FIRST_LAST_DUE
  
  # Loop through each due date and tag, scheduling tasks in order
  for dueDate, tag in tlSortedDueDates:
    if dueDate.lTasks:
      # Calculate the total available free time for the current day
      totalFreeTime = get_free_time(
        calendar,
        lastDueDate,
        dueDate.datetime.date(),
        startTime,
        stopTime,
        skipSat,
        skipSun,
      )

      if totalFreeTime == 0:
        print("No time available for planning")

      # Assign time to tasks based on available free time and tag ratios
      dAssignedTime = assign_time(dlTasks, dueDate, tag, totalFreeTime, dRatios)

      # Initialize the planned time dictionary for each tag (set to 1 by default)
      dPlannedTime = dict().fromkeys(dlTasks.keys(), 1)
        
      # Loop through each day between lastDueDate and the current dueDate
      while lastDueDate <= dueDate.datetime.date():
        # Only schedule tasks for weekdays (Monday to Friday) unless skipSat or skipSun is True
        if (
          not skipSat and lastDueDate.weekday() == 5  # Saturday
          or not skipSun and lastDueDate.weekday() == 6  # Sunday
          or lastDueDate.weekday() < 5  # Weekday (Mon-Fri)
        ):
          # Plan and schedule tasks for the day
          lScheduledTasks += plan_day(lastDueDate, startTime, stopTime, dAssignedTime, dPlannedTime, dlTasks, calendar)

        # Move to the next day
        lastDueDate += dt.timedelta(days=1)

  add_dates_to_parents(lScheduledTasks)

  # Return the list of scheduled tasks for the period
  return lScheduledTasks