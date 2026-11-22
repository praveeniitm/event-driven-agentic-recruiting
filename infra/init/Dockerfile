FROM python:3.11-slim
# PROD: pin by digest and install from a hashed lockfile.
RUN pip install --no-cache-dir confluent-kafka==2.5.3
COPY init.py /app/init.py
CMD ["python", "/app/init.py"]
