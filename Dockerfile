ARG SPARK_VERSION=v3.3.1
FROM apache/spark-py:${SPARK_VERSION}

# using root user
USER root:root

# create the directory that will store the spark jobs
RUN mkdir /src
RUN mkdir /src/main

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# copy spark jobs local to image
COPY src/main/. /src/main

# set python3
ENV PYSPARK_PYTHON=/usr/bin/python3

CMD ["python3", "src/main"]