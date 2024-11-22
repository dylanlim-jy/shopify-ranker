# Start with a base image containing your desired Python version
FROM python:3.9.2

# Set the working directory in the container
WORKDIR /home/dylan/shopify-ranker

# Copy your Python scripts into the container
COPY . .

# Install dependencies (if you have a requirements.txt file)
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Specify the command to run your Python script
CMD ["python", "prefect/shopify_apps.py"]
