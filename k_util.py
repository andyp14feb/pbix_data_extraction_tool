from datetime import datetime
from pathlib import Path
import csv
import hashlib
import json
import re
import shutil




def create_unique_output_folder(filename, source_path):
    """Create unique output folder with collision handling"""
    base_name = Path(filename).stem
    hash_suffix = hashlib.md5(source_path.encode()).hexdigest()[:8]
    output_folder = Path("output") / f"{base_name}_{hash_suffix}"
    output_folder.mkdir(exist_ok=True)
    return str(output_folder)


def get_current_datetime_str():
    """Returns current date and time as a string in format yyyymmdd_hhmm"""
    return datetime.now().strftime("%Y%m%d_%H%M")

def log_progress(message):
    """Log extraction information to extraction log file"""
    with open("output/extraction_log.txt", "a") as f:
        f.write(f"{get_current_datetime_str()} : {message}\n")
        print(f"{get_current_datetime_str()} : {message}\n")
        
def log_error(message):
    """Log errors to error log file"""
    with open("output/error_log.txt", "a") as f:
        message=f"ERROR : {message}"
        f.write(f"{get_current_datetime_str()} : {message}\n")
        log_progress(message)
        # print(f"{get_current_datetime_str()} : {message}\n")

def log_summary(pbix_path, output_folder, what):
    """Log mapping of PBIX file to output folder"""
    with open("output/summary_log.txt", "a") as f:
        log_line = f'{get_current_datetime_str()} : what="{what}", file="{pbix_path}", name="{Path(pbix_path).name}", output="{output_folder}"\n'
        f.write(log_line)

def clear_output_folder():
    """Clears all files and subdirectories inside the ./output folder."""
    output_dir = Path("output")
    if output_dir.exists() and output_dir.is_dir():
        for item in output_dir.iterdir():
            if item.is_file():
                item.unlink()  # Remove file
            elif item.is_dir():
                shutil.rmtree(item)  # Remove directory and its contents


def convert_summary_log_to_csv(summary_txt="output/summary_log.txt", csv_output="output/summary_log.csv"):
    """Convert summary_log.txt into summary_log.csv"""
    if not Path(summary_txt).exists():
        log_error(f"{summary_txt} not found.")
        return

    rows = []
    with open(summary_txt, "r", encoding="utf-8") as f:
        for line in f:
            match = re.match(r'(\d{8}_\d{4}) : what="([^"]+)", file="([^"]+)", name="([^"]+)", output="([^"]+)"', line)
            if match:
                datetime_str, what, file_path, file_name, output_path = match.groups()
                rows.append({
                    "datetime": datetime_str,
                    "what": what,
                    "file": file_path,
                    "name": file_name,
                    "output": output_path
                })
            else:
                log_error(f"Failed to parse line: {line.strip()}")

    with open(csv_output, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["datetime", "what", "file", "name", "output"])
        writer.writeheader()
        writer.writerows(rows)

    log_progress(f"Converted {summary_txt} to CSV at {csv_output}")
                

def convert_summary_log_to_json(summary_txt="output/summary_log.txt", json_output="output/summary_log.json"):
    """Convert summary_log.txt into summary_log.json"""
    if not Path(summary_txt).exists():
        log_error(f"{summary_txt} not found.")
        return

    entries = []
    with open(summary_txt, "r", encoding="utf-8") as f:
        for line in f:
            match = re.match(r'(\d{8}_\d{4}) : what="([^"]+)", file="([^"]+)", name="([^"]+)", output="([^"]+)"', line)
            if match:
                datetime_str, what, file_path, file_name, output_path = match.groups()
                entries.append({
                    "datetime": datetime_str,
                    "what": what,
                    "file": file_path,
                    "name": file_name,
                    "output": output_path
                })
            else:
                log_error(f"Failed to parse line: {line.strip()}")

    with open(json_output, "w", encoding="utf-8") as jf:
        json.dump(entries, jf, indent=2)

    log_progress(f"Converted {summary_txt} to JSON at {json_output}")


def list_tmdl_files_to_json(output_dir="output", json_output="output/tmdl_index.json"):
    """Scan all result folders for .tmdl files and save as JSON"""
    output_path = Path(output_dir)
    data = []

    if not output_path.exists():
        log_error(f"{output_dir} does not exist.")
        return

    for result_folder in output_path.iterdir():
        if result_folder.is_dir():
            tables_dir = result_folder / "Model" / "tables"
            if tables_dir.exists():
                for tmdl_file in tables_dir.glob("*.tmdl"):
                    data.append({
                        "resultFolder": result_folder.name,
                        "tmdl_path": str(tmdl_file),
                        "tmdl_name": tmdl_file.name
                    })

    with open(json_output, "w", encoding="utf-8") as jf:
        json.dump(data, jf, indent=2)

    log_progress(f"Saved .tmdl list to JSON at {json_output}")

def print_tmdl_contents():
    """Reads tmdl_index.json, iterates, and prints the content of each tmdl file."""
    try:
        with open(".\\output\\tmdl_index.json", "r") as f:
            tmdl_index = json.load(f)
    except FileNotFoundError:
        print("Error: tmdl_index.json not found.")
        return
    except json.JSONDecodeError:
        print("Error: Could not parse tmdl_index.json.  Invalid JSON.")
        return

    for entry in tmdl_index:
        tmdl_path = Path(entry["tmdl_path"])
        try:
            with open(tmdl_path, "r") as tmdl_file:
                tmdl_content = tmdl_file.read()
            print(f"Contents of {tmdl_path}:")
            print(tmdl_content)
            print("-" * 40)  # Separator between files
        except FileNotFoundError:
            print(f"Error: {tmdl_path} not found.")
        except Exception as e:
            print(f"Error reading {tmdl_path}: {e}")

import json
from pathlib import Path

def extract_partitions_from_database_json(output_dir="output", summary_file="output/partition_summary.json"):
    """Extract partition details from all database.json files into a single summary JSON."""
    result = []
    output_path = Path(output_dir)

    if not output_path.exists():
        print(f"{output_dir} does not exist.")
        return

    for result_folder in output_path.iterdir():
        if not result_folder.is_dir():
            continue

        db_json_path = result_folder / "Model" / "database.json"
        if not db_json_path.exists():
            continue

        try:
            with open(db_json_path, "r", encoding="utf-8") as f:
                db_data = json.load(f)

            tables = db_data.get("model", {}).get("tables", [])
            for table in tables:
                partitions = table.get("partitions", [])
                for part in partitions:
                    result.append({
                        "pbix": result_folder.name,
                        "partition_name": part.get("name"),
                        "partition_mode": part.get("mode"),
                        "partition_expression": part.get("source", {}).get("expression")
                    })

        except Exception as e:
            print(f"Error reading {db_json_path}: {e}")

    # Save summary
    with open(summary_file, "w", encoding="utf-8") as out:
        json.dump(result, out, indent=2)

    print(f"✅ Partition summary saved to {summary_file}")
