import argparse
import hashlib
import json
import logging
import os
import shutil
import time
from typing import Optional


def calculate_md5(file_path: str) -> Optional[str]:
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"Error calculating MD5 for {file_path}: {e}")
        return None


def read_metadata(meta_file: str) -> int:
    if os.path.exists(meta_file):
        with open(meta_file, "r") as f:
            return int(f.read())
    return 0


def write_metadata(meta_file: str, bytes_transferred: int) -> None:
    metadata = {'bytes_transferred': bytes_transferred}
    try:
        with open(meta_file, "wb") as f:
            f.write(json.dumps(metadata).encode('utf-8'))
    except Exception as e:
        print(f"An error occurred while writing metadata: {e}")


def create_or_update_file(src_file: str, replica_file: str, buffer_size: int = 4096) -> None:
    try:
        meta_file = f"{replica_file}.meta"

        if not os.path.exists(replica_file) or calculate_md5(src_file) != calculate_md5(replica_file):
            logging.info(f"Copying {src_file} to {replica_file}")

            start_point = read_metadata(meta_file)

            with open(src_file, "rb") as src:
                src.seek(start_point)
                with open(replica_file, "ab") as dest:
                    while True:
                        chunk = src.read(buffer_size)
                        if not chunk:
                            break
                        dest.write(chunk)
                        start_point += len(chunk)
                        write_metadata(meta_file, start_point)

            if os.path.exists(meta_file):
                os.remove(meta_file)

    except Exception as e:
        logging.error(f"Error copying file {src_file} to {replica_file}: {e}")


def remove_file_or_folder(path: str) -> None:
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        logging.info(f"Removed {path}")
    except Exception as e:
        logging.error(f"Error removing {path}: {e}")


def sync_folders(src: str, replica: str) -> None:
    try:
        for foldername, _, filenames in os.walk(src):
            relative_folder = os.path.relpath(foldername, src)
            replica_folder = os.path.join(replica, relative_folder)

            if not os.path.exists(replica_folder):
                logging.info(f"Creating folder {replica_folder}")
                os.makedirs(replica_folder)

            for filename in filenames:
                src_file = os.path.join(foldername, filename)
                replica_file = os.path.join(replica_folder, filename)
                create_or_update_file(src_file, replica_file)

        for foldername, _, filenames in os.walk(replica):
            relative_folder = os.path.relpath(foldername, replica)
            src_folder = os.path.join(src, relative_folder)

            if not os.path.exists(src_folder):
                remove_file_or_folder(foldername)
                continue

            for filename in filenames:
                replica_file = os.path.join(foldername, filename)
                src_file = os.path.join(src_folder, filename)

                if not os.path.exists(src_file):
                    remove_file_or_folder(replica_file)
    except Exception as e:
        logging.error(f"Error during synchronization: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="veeam synchronize two folders.")
    parser.add_argument("--config", help="Path to JSON configuration file")
    args = parser.parse_args()

    if args.config:
        with open(args.config, "r") as f:
            config = json.load(f)
            src = config["src"]
            replica = config["replica"]
            interval = config.get("interval", 1)
            log_path = config.get("log", "sync.log")
    else:
        src = input("Enter the source folder path: (default is current directory)") or "."
        replica = input("Enter the replica folder path: ")
        minutes_interval = int(input("Enter the synchronization interval in minutes: (default is 1)") or 1)
        log_path = input("Enter the log file path (default is sync.log): ") or "sync.log"

    logging.basicConfig(filename=log_path, level=logging.INFO)

    while True:
        logging.info("Starting synchronization")
        sync_folders(src, replica)
        logging.info(f"Synchronization complete, sleeping for {minutes_interval} minute")
        time.sleep(minutes_interval * 60)
