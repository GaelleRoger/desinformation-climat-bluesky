FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY prepare_and_launch.py .
COPY load_results.py .
ENTRYPOINT ["python"]
CMD ["prepare_and_launch.py"]
