FROM python:3.11-slim
WORKDIR /work
COPY file_batch/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock
COPY file_batch /work/file_batch
COPY transform /work/transform
ENTRYPOINT ["python", "-m", "file_batch"]
