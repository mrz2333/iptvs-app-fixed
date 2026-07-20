FROM python:3.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD ["python", "app.py"]
