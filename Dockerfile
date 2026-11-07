FROM astrocrpublic.azurecr.io/runtime:3.0-6

# pymongo is required for the MongoDB load task
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
