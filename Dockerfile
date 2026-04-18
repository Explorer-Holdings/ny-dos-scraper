# NY DOS Business Entity Scraper — Dockerfile
# Uses Apify's Playwright base image which includes Chromium and all system deps.

FROM apify/actor-python-playwright:3.12

# Copy project files
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

COPY . ./

# Apify expects the entry point to be main.py
CMD ["python", "main.py"]
