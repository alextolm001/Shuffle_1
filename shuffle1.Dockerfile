FROM python:3.8-bullseye

# Installeer Java, Git en tools die nodig zijn voor PySpark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk git && \
    apt-get clean

# Stel JAVA_HOME in voor PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Installeer benodigde Python-bibliotheken
RUN pip install --no-cache-dir pyspark \
    google-api-python-client \
    google-auth \
    google-auth-httplib2 \
    google-auth-oauthlib

# Zet werkmap
WORKDIR /app

# Clone je GitHub-repo (openbare repo) en kopieer het script
RUN git clone https://github.com/alextolm001/eurovision.git /app/code && \
    cp /app/code/count_votes.py .

# Het service account wordt later gemount
CMD ["python", "count_votes.py"]