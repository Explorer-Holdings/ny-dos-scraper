FROM apify/actor-python-playwright:3.12

# Force-upgrade pydantic FIRST before anything else installs/uses it.
# The base image ships with an older pydantic that conflicts with crawlee._types.
RUN pip install --no-cache-dir --upgrade "pydantic>=2.7.0,<3.0.0"

# Now install the rest of the dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

COPY ../../../Downloads ./

CMD ["python", "main.py"]
