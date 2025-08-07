import os
import sys
import requests
import logging
from zipfile import ZipFile
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logger, format_time

def setup_logger(log_file_name="extract.log"):
    logger = logging.getLogger("extract_logger")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s'
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    fh = logging.FileHandler(log_file_name)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


def download_zip_file(url, output_dir, logger):
    try:
        logger.info(f"Starting download from {url}")
        response = requests.get(url, stream=True)
        os.makedirs(output_dir, exist_ok=True)
        if response.status_code == 200:
            filename = os.path.join(output_dir, "download.zip")
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded zip file: {filename}")
            return filename
        else:
            raise Exception(f"Failed to download file: status code {response.status_code}")
    except Exception as e:
        logger.error(f"Error downloading zip file: {e}")
        raise


def extract_zip_file(zip_filename, output_dir, logger):
    try:
        with ZipFile(zip_filename, "r") as zip_file:
            zip_file.extractall(output_dir)
        logger.info(f"Extracted files written to: {output_dir}")
        logger.info("Removing the zip file...")
        os.remove(zip_filename)  # Remove the zip file after extraction
    except Exception as e:
        logger.error(f"Error extracting zip file: {e}")
        raise


def fix_json_dict(output_dir, logger):
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    if not os.path.exists(file_path):
        err_msg = f"{file_path} does not exist."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        output_path = os.path.join(output_dir, "fixed_da.json")
        with open(output_path, "w", encoding="utf-8") as f_out:
            for key, value in data.items():
                record = {"id": key, "related_ids": value}
                json.dump(record, f_out, ensure_ascii=False)
                f_out.write("\n")

        logger.info(f"Fixed JSON written to: {output_path}")
        logger.info("Removing the original file...")
        os.remove(file_path)

    except Exception as e:
        logger.error(f"Error fixing JSON dict: {e}")
        raise


if __name__ == "__main__":
    logger = setup_logger()

    if len(sys.argv) < 2:
        logger.error("Extraction path is required.")
        logger.info("Example Usage:")
        logger.info("python3 execute.py /home/ardent-sharma/Data/Extraction")
        sys.exit(1)

    EXTRACT_PATH = sys.argv[1]

    if not os.path.exists(EXTRACT_PATH):
        logger.error(f"Extraction path {EXTRACT_PATH} does not exist.")
        sys.exit(1)

    try:
        logger.info("Starting Extraction Engine...")
        start_time =time.time()

        KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250710%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250710T025309Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=a7c62d2e9fd3587d7d57f6402e9269d27ddf7952bdf62477d3f64fa27d04f246fd54aaf8a0e5ab8344682f668db61316c8cbeb3a0192249f1b2a840ad89d4580b0908369e8915c89f3418af5718bf447b8efaed4537f0b819615091dfcd04557b45acf0f4115cba0012621e89a3b9644fe147d8d7c8a79c69fa35ca7fee62bbf62e28b7f425be4a69726aa0eaa1aa6b5c17778e682fd035b7fc0e004868eb54fc756a5d597c5a1375bdafb7244ee31262291c2728e865225cb2ce1f56228922eff7265fe5dd35231d0a4b8c3ce16364dade61febdb7ccfc715adec6e7981fe213d17ee32cbc3d6a1686d8713306221e335547f32bc965bb6b96dcfa0c00993e3"  
        zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH, logger)
        extract_zip_file(zip_filename, EXTRACT_PATH, logger)
        fix_json_dict(EXTRACT_PATH, logger)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info("Extraction Successfully Completed!!!")
        logger.info(f"Total time taken {format_time(elapsed_time)}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)
