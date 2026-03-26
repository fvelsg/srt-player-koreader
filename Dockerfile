# Use a lightweight Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script into the container
COPY server.py .

# Expose the port your server uses
EXPOSE 8000

# Run the script. The -u flag forces unbuffered output so logs show up immediately
CMD ["python", "-u", "server.py"]