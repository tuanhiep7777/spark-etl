# Use a Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script into the container
COPY --chmod=644 generate_data.py /generate_data.py

# change mode
RUN chmod +x /generate_data.py
# Install any necessary dependencies (if needed)
RUN pip install pymongo

# Command to run the Python script
CMD ["python", "/generate_data.py"]