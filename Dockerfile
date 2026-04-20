FROM python:3.11
WORKDIR /app


RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/  && \
    pip config set global.trusted-host mirrors.aliyun.com

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app

CMD ["bash", "-c", "echo 'Python ETL BEGIN' && sleep infinity"]