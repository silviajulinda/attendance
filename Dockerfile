FROM python:3

ADD attendance_etl.py /
ADD course.csv /
ADD Academic_calendar.csv /
ADD course_attendance.csv /
ADD enrollment.csv /
ADD schedule.csv /

RUN pip install pandas

CMD [ "python", "./attendance_etl.py" ]