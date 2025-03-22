FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the producer script
COPY producer.py /app/

# Install dependencies
RUN pip install fastapi uvicorn requests kazoo python-multipart

# Expose the FastAPI server port
EXPOSE 5000

# Command to run the producer service
CMD ["uvicorn", "producer:app", "--host", "0.0.0.0", "--port", "5000"]
