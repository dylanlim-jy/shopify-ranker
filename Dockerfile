# Start with a base image containing your desired Python version
FROM python:3.9.2

# Set the working directory in the container
WORKDIR /home/dylan/shopify-ranker

# Copy your Python scripts into the container
COPY . .

# Install pip dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Install playwright dependencies; note that there may be
# an issue depending on the OS of the VPS; use apt-get if necessary.
RUN playwright install
RUN apt-get install libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 libatspi2.0-0

# Run Prefect worker in the background
CMD ["prefect", "worker", "start", "--pool", "process-work-pool"]
