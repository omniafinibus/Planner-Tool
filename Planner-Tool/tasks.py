import datetime as dt
import os
import uuid
import re

HIGH_MULTIPLIER = 2
MINUTE_PER_PAGE = 60 / 20

_FORMAT = [
  "task",
  "tag",
  "startDate",
  "dueDate",
  "priority",
  "description",
  "time"
]


class Task:
  def __init__(self, text: str = None, parent=None):
    self.task = ""
    self.startDate = None
    self.dueDate = None
    self.description = ""
    self.priority = 0
    self.tag = ""
    self.uid = str(uuid.uuid1())
    self.parent = None
    self.time = None
    if text is not None:
      self.set_task_from_text(text, parent)

  def set_task_from_text(self, text: str, parent=None):
    if parent:
      self.parent = parent.uid
    lRawData = text.split(",")
    dData = {_FORMAT[i]: lRawData[i] for i in range(len(_FORMAT))}
    for key, value in dData.items():
      value = re.sub(r"^\s*|\s*$", "", value)
      self.__dict__[key] = value
    self.startDate = convert_date_or_time(self.startDate)
    self.dueDate = convert_date_or_time(self.dueDate)
    if self.dueDate:
        self.dueDate = self.dueDate-dt.timedelta(days=1) # required to make sure tasks are planned BEFORE the deadline
    self.priority = int(self.priority)
    rawTime = re.sub(r"\s", "", self.time)
    if re.findall("min", self.time):
      rawTime = re.sub(r"[a-z]", "", rawTime)
      hour = int(int(rawTime) / 60)
      remMin = int(int(rawTime) % 60)
      self.time = dt.timedelta(hours=hour, minutes=remMin)
    elif re.findall("page", self.time):
      self.description + "\n" + self.time
      multiplier = HIGH_MULTIPLIER if re.findall("high", self.time) else 1
      rawTime = re.sub(r"[a-z]", "", rawTime)
      minutes = multiplier * MINUTE_PER_PAGE * int(rawTime)
      hour = int(minutes / 60)
      remMin = int(minutes % 60)
      self.time = dt.timedelta(hours=hour, minutes=remMin)

  def __repr__(self):
    return f"{self.task} | {self.startDate} | {self.dueDate} | {self.time}"


def convert_date_or_time(dateTime: str):
  if re.findall(r"[0-9]*\/[0-9]*\/[0-9]* [0-9]*:[0-9]*", dateTime):
    return dt.datetime.strptime(dateTime, "%d/%m/%Y %H:%M")
  elif re.findall(r"[0-9]*\/[0-9]*\/[0-9]*", dateTime):
    return dt.datetime.strptime(dateTime, "%d/%m/%Y")
  else:
    return None


def create_tasks_from_todo(todoDirectory):
  if not os.path.isfile(todoDirectory):
    print(f"File does not exist: {todoDirectory}")
    return list()
  else:
    print(f"Opening file: {todoDirectory}")
    with open(todoDirectory) as f:
      lLines = f.readlines()
      lLines.pop(0)
      lTasks = list()
      parent = None
      for line in lLines:
        if re.search(r"^  ", line):
          lTasks.append(Task(line, parent if parent else None))
        else:
          parent = Task(line)
          lTasks.append(parent)
    return lTasks


def create_task_ics(task: Task):
  if len(task.task) > 75:
    print(f"Task: {task.task} will result in faulty ics, task too long")
  if len(task.description) > 75:
    print(f"Task: {task.task} will result in faulty ics, description too long")
  text = "".join(
    [
      f"BEGIN:VTODO\r\n",
      f"DTSTAMP:20241117T215427Z\r\n",
      f"UID:{re.sub('-','',str(task.uid).upper())}\r\n",
      f"SEQUENCE:4\r\n",
      f"CREATED:20241021T230800Z\r\n",
      f"LAST-MODIFIED:20241117T215426Z\r\n",
      f"SUMMARY:{task.task}\r\n",
      f"DESCRIPTION:{task.description}\r\n",
      f"PRIORITY:{task.priority}\r\n",
      f"CATEGORIES:{task.tag}\r\n",
    ]
  )
  if task.parent is not None:
    text += (
      f"RELATED-TO;RELTYPE=PARENT:{re.sub('-','',str(task.parent).upper())}\r\n"
    )
    
  if task.startDate is not None:
    if isinstance(task.startDate, dt.datetime):
      t = task.startDate.time()
      d = task.startDate.date()
      text += (
        f"DTSTART:{d.year}{d.month:02}{d.day:02}T{t.hour-1:02}{t.minute:02}00Z\r\n"
      )
    elif isinstance(task.startDate, dt.date):
      d = task.startDate
      text += (
        f"DTSTART;VALUE=DATE:{d.year}{d.month:02}{d.day:02}\r\n"
      )
         
  if task.dueDate is not None:
    if isinstance(task.dueDate, dt.datetime):
      t = task.dueDate.time()
      d = task.dueDate.date()
      text += f"DUE:{d.year}{d.month:02}{d.day:02}T{t.hour-1:02}{t.minute:02}00Z\r\n"
    elif isinstance(task.dueDate, dt.date):
      d = task.dueDate
      text += (
        f"DUE;VALUE=DATE:{d.year}{d.month:02}{d.day:02}\r\n"
      )
  text += "END:VTODO\r\n"
  return text


def create_ics_text(lTasks: list[Task]):
  return (
    "".join(
      [
        "BEGIN:VCALENDAR\r\n",
        "VERSION:2.0\r\n",
        "CALSCALE:GREGORIAN\r\n",
        "PRODID:-//SabreDAV//SabreDAV//EN\r\n",
        "X-WR-CALNAME:Planned tasks\r\n",
        "X-APPLE-CALENDAR-COLOR:#F1DB50\r\n",
        "REFRESH-INTERVAL;VALUE=DURATION:PT4H\r\n",
        "X-PUBLISHED-TTL:PT4H\r\n",
      ]
    )
    + "".join([create_task_ics(task) for task in lTasks])
    + "END:VCALENDAR"
  )