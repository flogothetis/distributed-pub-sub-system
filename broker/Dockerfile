FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the broker script
COPY broker.py /app/

# Install dependencies
RUN pip install fastapi uvicorn kazoo requests python-multipart

# Expose the FastAPI server port
EXPOSE 5000

# Command to run the broker server
CMD ["uvicorn", "broker:app", "--host", "0.0.0.0", "--port", "5000"]