FROM python:3.10-slim

WORKDIR /app

# install deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# copy your code
COPY main.py ./

# tell Cloud Run which port
ENV PORT 8080

# run as non-root
USER 1000

# start
CMD ["python", "main.py"]
