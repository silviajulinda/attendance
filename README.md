# Introduction

Data pipeline using python

### Prerequisites
* Python 
* Pandas library
* Docker

### Executing program
```
python attendance_etl.py
```
### Program output
new directory named "result" will be created with cvs files

### Build and run docker image
1. Build the image
```
docker build -t attendance-etl .
```
2. Run the image
```
docker run --rm -it -v //c/code/result:/result/ attendance-etl
```

Change the mount directory to your local disk
```
docker run --rm -it -v <your local disk directory>:/result/ attendance-etl
```



