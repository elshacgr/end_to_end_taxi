import requests
import shutil
import os

links = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet"
]

# local path
source_file_path = '/mnt/c/Users/USER/airflow/dags/data/'

# Iterate over each link
for link in links:
    # Send a GET request to download the file
    response = requests.get(link)

    # Extract the filename from the URL
    filename = link.split("/")[-1]

    # Construct the full file path by joining folder path and filename
    file_path = os.path.join(source_file_path, filename)

    if os.path.exists(file_path):
        os.remove(file_path)
    # Check if the request was successful
    if response.status_code == 200:
        # Save the response content (file) to local direct
        with open(file_path, "wb") as file:
            file.write(response.content)
    
        print(f"File '{filename}' downloaded successfully.")
    else:
        print(f"Failed to download file from '{link}'.")

print("All files have been downloaded.")

# # copy data to /home/elshacgr/taxidata
destination = "/home/elshacgr/taxidata/"

# Get a list of all files in the source folder
files = os.listdir(source_file_path)

# Iterate over each file and copy it to the destination folder
for file in files:
    source_file_paths = os.path.join(source_file_path, file)
    if os.path.isfile(source_file_paths):
        shutil.copy(source_file_paths, destination)

# save to hdfs so it can be accessed with hive
import subprocess

hdfs_folder_path = '/user/elshacgr/data'
subprocess.run(['hadoop', 'fs', '-put', destination, hdfs_folder_path])
