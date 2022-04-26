# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime
import sqlite3
import glob
import os
import os.path
from datetime import date

#directory for data layers
cwd = os.getcwd()
pathStaging = cwd + '/result/staging_data/'
pathStagingCourse = cwd + '/result/staging_data/course/'
pathStagingCalendar = cwd + '/result/staging_data/Academic_calendar/'
pathStagingAttendance = cwd + '/result/staging_data/course_attendance/'
pathStagingEnrollment = cwd + '/result/staging_data/enrollment/'
pathStagingSchedule = cwd + '/result/staging_data/schedule/'
pathDwh = cwd + '/result/dwh_data/'
pathDatamart = cwd + '/result/datamart/'

#table names
course = 'course'
academic_calendar = 'Academic_calendar'
course_attendance = 'course_attendance'
enrollment = 'enrollment'
schedule = 'schedule'

#datawarehouse data model
dwh_courseDim = pathDwh + 'courseDim.csv'
dwh_calendarDim = pathDwh + 'calendarDim.csv'
dwh_attendanceFact = pathDwh + 'attendanceFact.csv'
dwh_enrollmentFact = pathDwh + 'enrollmentFact.csv'
dwh_scheduleDim = pathDwh + 'scheduleDim.csv'

#Function to create directory
def create_dir (path):
  isExist = os.path.exists(path)
  if not isExist:
    print('create directory: ' + path)
    os.makedirs(path, exist_ok=False)

#Function to move data from source to staging data layer
def load_to_staging(table):
  cwd = os.getcwd()
  filename = cwd + '/' + table + '.csv'
  df = pd.read_csv(filename)
  print(df.head(3))
  today = date.today()
  staging_dir = cwd + '/result/staging_data/' + table + '/' + table + '-' + str(today) + '.csv'
  print('write to: ' +  staging_dir + '\n')
  df.to_csv( staging_dir, index=False)

#Function to write to datawarehouse
def write_dwh (staging_path, schema, dwh):
  #get latest file
  list_of_files = glob.glob(staging_path) # * means all if need specific format then *.csv
  latest_file = max(list_of_files, key=os.path.getctime)

  df = pd.read_csv(latest_file)
  print(df.head(3))
  df.columns = schema
  print('write to: ' +  dwh)
  df.to_csv (dwh, index=False)

#Make directory for data layers
create_dir(pathStaging)
create_dir(pathStagingCourse)
create_dir(pathStagingCalendar)
create_dir(pathStagingAttendance)
create_dir(pathStagingEnrollment)
create_dir(pathStagingSchedule)
create_dir(pathDwh)
create_dir(pathDatamart)

#move data from source to staging data layer appending the date
load_to_staging(course)
load_to_staging(academic_calendar)
load_to_staging(course_attendance)
load_to_staging(enrollment)
load_to_staging(schedule)

#Transform data and load to dwh

#Course Dimension
#get latest file
list_of_files = glob.glob(pathStagingCourse + '/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)

df = pd.read_csv(latest_file)
print(df.head(3))
df.columns = ['Course_id', 'Course_name']
print('write to: ' +  dwh_courseDim + '\n')
df.to_csv (dwh_courseDim, index=False)

#Enrollment Fact
#get latest file
list_of_files = glob.glob(pathStagingEnrollment + '/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)

df = pd.read_csv(latest_file)
print(df.head(3))
df.columns = ['Enrollment_id', 'Student_id', 'Schedule_id', 'Academic_year', 'Semester_id', 'Enroll_dt']
print('write to: ' +  dwh_enrollmentFact + '\n')
df.to_csv (dwh_enrollmentFact, index=False)

#Calendar Dimension
list_of_files = glob.glob(pathStagingCalendar + '/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)
df = pd.read_csv(latest_file)

df['Week_start_dt']= pd.to_datetime(df['Week_start_dt'], format='%d/%m/%Y')
df['Week_end_dt']= pd.to_datetime(df['Week_end_dt'], format='%d/%m/%Y')

df.columns = ['Academic_year', 'Semester_id', 'Week_id', 'Week_start_dt', 'Week_end_dt']
print(df.head(3))
print('write to: ' + dwh_calendarDim + '\n')
df.to_csv (dwh_calendarDim, index=False)

#Attendance Fact
#Create column Attend_dt_day based on Attend_dt
list_of_files = glob.glob(pathStagingAttendance + '/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)

df = pd.read_csv(latest_file)

result = []
for value in df['ATTEND_DT']:
    #Monday:0 Sunday=6
    attend_dt_day = datetime.strptime(value, '%d-%b-%y').weekday() 
    result.append(attend_dt_day + 2)

df['COURSE_DAYS'] = result
df['ATTEND_DT']= pd.to_datetime(df['ATTEND_DT'], format='%d-%b-%y')

df.columns = ['Attendance_id', 'Student_id', 'Schedule_id', 'Attend_dt', 'Course_days']
print(df.head(3))
print('write to: ' + dwh_attendanceFact + '\n')
df.to_csv (dwh_attendanceFact, index=False)

#Schedule Dimension
#Explode column Course_days to separate rows
list_of_files = glob.glob(pathStagingSchedule + '/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)

df = pd.read_csv(latest_file)

df2=df.assign(COURSE_DAYS=df['COURSE_DAYS'].str.split(',')).explode('COURSE_DAYS')
df2.columns = ['Schedule_id','Course_id', 'Lecturer_id', 'Start_dt', 'End_dt', 'Course_days']
print(df2.head(3))
print('write to: ' + dwh_scheduleDim + '\n')
df2.to_csv (dwh_scheduleDim, index=False)

#Datamart layer for reporting
df_scheduleDim = pd.read_csv(dwh_scheduleDim)
df_courseDim = pd.read_csv(dwh_courseDim)
df_attendanceFact = pd.read_csv(dwh_attendanceFact)
df_enrollmentFact = pd.read_csv(dwh_enrollmentFact)
df_calendarDim = pd.read_csv(dwh_calendarDim)

result = pd.merge(df_scheduleDim, df_courseDim, on="Course_id")
result2 = pd.merge(result, df_attendanceFact, on=["Schedule_id", "Course_days"])
result3 = pd.merge(result2, df_enrollmentFact, on=["Student_id", "Schedule_id"])

#Make the db in memory
conn = sqlite3.connect(':memory:')
#write the tables
result3.to_sql('result3', conn, index=False)
df_calendarDim.to_sql('calendar', conn, index=False)

qry = '''
    select  
        Schedule_id, Course_id, Course_name, Attendance_id, Student_id, Attend_dt,"Course_days",
        calendar.Academic_year, calendar.Semester_id, Week_id, Week_start_dt, Week_end_dt
    from
        result3 join calendar on
        Attend_dt between Week_start_dt and Week_end_dt 
        and result3.Academic_year = calendar.Academic_year
        and result3.Semester_id = calendar.Semester_id
    '''

attendance_yes = pd.read_sql_query(qry, conn)    

result4 = pd.merge(df_enrollmentFact, df_calendarDim, on=["Academic_year", "Semester_id"])
result5 = pd.merge(result4, df_scheduleDim, on=["Schedule_id"])
total_attendance = pd.merge(result5, df_courseDim, on=["Course_id"])

final_dataset = pd.merge(total_attendance, attendance_yes[["Schedule_id", "Student_id", "Academic_year", "Semester_id", "Week_id", "Course_id","Attend_dt", "Course_days"]], how="left", on=["Schedule_id", "Student_id", "Academic_year", "Semester_id", "Week_id", "Course_id", "Course_days"])
print(final_dataset.head(3))
print('write to: ' + pathDatamart + 'attendance_datamart.csv')
final_dataset.to_csv (pathDatamart + 'attendance_datamart.csv', index=False)

#Reporting
#Make the db in memory
conn = sqlite3.connect(':memory:')
#write the tables
final_dataset.to_sql('final_dataset', conn, index=False)

qry = '''
    select Semester_id, Academic_year, Week_id, Course_name, 
    (round(attend,2) / total_attendance) * 100 as Attendance_pct from (
    select  
        Semester_id, Academic_year, Week_id, Course_name, 
        sum(case when Attend_dt is not null then 1 end ) as attend, count(Enrollment_id) as total_attendance
    from final_dataset
    group by
        Semester_id, Academic_year, Week_id, Course_name ) A
    '''

reports = pd.read_sql_query(qry, conn)    
print(reports.head(3))
print('write to: ' + pathDatamart + 'report.csv')
reports.to_csv (pathDatamart + 'report.csv', index=False)