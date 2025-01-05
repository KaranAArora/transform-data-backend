# Use a lightweight Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /backend

# Set the Python path to include the src folder inside the backend directory
ENV PYTHONPATH=/backend/src

# Copy only the requirements.txt first (dependencies rarely change compared to app code)
COPY requirements.txt /backend/

# Install dependencies only if requirements.txt has changed
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . /backend

# Set the environment variable for Flask (if using Flask, adjust accordingly)
ENV FLASK_ENV=production

# Expose the port the app will run on
EXPOSE 5000

# Start the app with Gunicorn and UvicornWorker
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "src.app:app", "--bind", "0.0.0.0:5000"]