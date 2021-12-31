import requests
import csv
import io
from zipfile import ZipFile
import os
from concurrent.futures import ThreadPoolExecutor


def download_csv_tozip(url):
    """Downloads csv, adding audit field with name, save as zip to disk"""

    try:
        # Download file from link
        print("Downloading file...")
        download = requests.get(url)
        print("File successfully downloaded")

        # Splits the csv file, creates the in-memory output file and the csv writer object
        lines = download.text.splitlines()
        output = io.StringIO()
        writer = csv.writer(output)

        # Reads the input file and adds the edited header to the output file
        print("Editing started...")
        c = csv.reader(lines)
        headers = next(c)
        headers.append("Audit")
        writer.writerow(headers)

        # Writes every line from the input to the output file with the new column
        for row in c:
            row.append("vasp")
            writer.writerow(row)

        print("Editing successfully completed")

        # Create zip file and adds csv
        print("Creating zip file...")
        filename = os.path.basename(url)
        with ZipFile(filename + '.zip', 'w') as zip:
            zip.writestr(filename + '.csv', output.getvalue())
        print(filename + ".zip is successfully created")

    except Exception as e:
        print("The following error happened: ", e)
        exit(1)


def download_with_threads(urls):
    """ Create a thread pool and download specified urls"""

    with ThreadPoolExecutor(max_workers=3) as executor:
        return executor.map(download_csv_tozip, urls, timeout=60)


if __name__ == "__main__":
    print("Starting program on multiple threads...")

    # CSV files to be downloaded
    urls = (
        "https://covid19.who.int/WHO-COVID-19-global-data.csv",
        "https://covid19.who.int/WHO-COVID-19-global-table-data.csv",
        "https://covid19.who.int/who-data/vaccination-data.csv",
        "https://covid19.who.int/who-data/vaccination-metadata.csv",
    )

    download_with_threads(urls)
