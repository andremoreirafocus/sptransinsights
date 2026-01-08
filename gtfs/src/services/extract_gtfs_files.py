import requests
import zipfile
import io


def extract_gtfs_files(url, login, password, downloads_folder):

    response = requests.get(url, auth=(login, password))
    if response.status_code == 404:
        print("Check credentials or portal access")
        return
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        print(z.namelist())  # List files: agency.txt, stops.txt, etc.
        z.extractall(downloads_folder)
    print(f"GTFS files extracted to {downloads_folder}")