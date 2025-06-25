from datetime import datetime
from pathlib import Path
import csv
import hashlib
import json
import re
import shutil
import pandas as pd




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



# def filter_and_classify_partitions(input_file='output/partition_summary.json',
#                                    output_file='output/partition_summary_filteredA.json'):
#     with open(input_file, 'r', encoding='utf-8') as f:
#         data = json.load(f)

#     filtered = []

#     for item in data:
#         expr = item.get("partition_expression", "")
#         if isinstance(expr, list):
#             expr_joined = "\n".join(expr)
#         else:
#             expr_joined = expr

#         # Only include if both 'let' and 'Source =' exist
#         if "let" in expr_joined and "Source =" in expr_joined:
#             new_item = {
#                 "pbix": item.get("pbix"),
#                 "partition_name": item.get("partition_name"),
#                 "partition_mode": item.get("partition_mode"),
#                 "partition_type": None,
#                 "sheet_table_location": None,
#                 "sheet_table_name": None,
#                 "partition_expression": item.get("partition_expression")
#             }

#             # Excel detection
#             if "Excel.Workbook" in expr_joined:
#                 new_item["partition_type"] = "excel"

#                 # Get file path
#                 file_match = re.search(r'File\.Contents\("([^"]+)"\)', expr_joined)
#                 if file_match:
#                     new_item["sheet_table_location"] = file_match.group(1)

#                 # Get sheet name
#                 sheet_match = re.search(r'\[Item\s*=\s*"([^"]+)"', expr_joined)
#                 if sheet_match:
#                     new_item["sheet_table_name"] = sheet_match.group(1)

#             # Database detection
#             elif ".Contents(" in expr_joined:
#                 new_item["partition_type"] = "database"

#                 # Get DB source
#                 db_match = re.search(r'Contents\("([^"]+)"', expr_joined)
#                 if db_match:
#                     new_item["sheet_table_location"] = db_match.group(1)

#                 # Get table name
#                 table_match = re.search(r'Name\s*=\s*"([^"]+)"', expr_joined)
#                 if table_match:
#                     new_item["sheet_table_name"] = table_match.group(1)

#             else:
#                 new_item["partition_type"] = "unknown"

#             filtered.append(new_item)

#     # Save as JSON
#     with open(output_file, 'w', encoding='utf-8') as f:
#         json.dump(filtered, f, indent=2)

#     print(f"✅ Done. Saved to {output_file}")



def filter_and_classify_partitions(input_file='output/partition_summary.json',
                                          summary_log_csv='output/summary_log.csv',
                                          output_file='output/partition_summary_filteredA.json'):
    # Load JSON input
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Load CSV mapping summary_log
    summary_df = pd.read_csv(summary_log_csv)
    summary_df['output_folder_name'] = summary_df['output'].apply(lambda x: Path(x).parts[-1])

    enriched_data = []

    for item in data:
        expr = item.get("partition_expression", "")
        if isinstance(expr, list):
            expr_joined = "\n".join(expr)
        else:
            expr_joined = expr

        if "let" in expr_joined and "Source =" in expr_joined:
            pbix_folder_name = item.get("pbix")
            matched_row = summary_df[summary_df['output_folder_name'] == pbix_folder_name]

            if not matched_row.empty:
                pbix_file_name = matched_row.iloc[0]['name']
                pbix_file_path = matched_row.iloc[0]['file']
            else:
                pbix_file_name = "UNKNOWN"
                pbix_file_path = "UNKNOWN"

            new_item = {
                "pbix_file_name": pbix_file_name,
                "pbix_full_path": pbix_file_path,
                "pbix": pbix_folder_name,
                "partition_name": item.get("partition_name"),
                "partition_mode": item.get("partition_mode"),
                "partition_type": None,
                "sheet_table_location": None,
                "sheet_table_name": None,
                "partition_expression": item.get("partition_expression")
            }

            if "Excel.Workbook" in expr_joined:
                new_item["partition_type"] = "excel"
                file_match = re.search(r'File\.Contents\("([^"]+)"\)', expr_joined)
                if file_match:
                    new_item["sheet_table_location"] = file_match.group(1)
                sheet_match = re.search(r'\[Item\s*=\s*"([^"]+)"', expr_joined)
                if sheet_match:
                    new_item["sheet_table_name"] = sheet_match.group(1)
            elif ".Contents(" in expr_joined:
                new_item["partition_type"] = "database"
                db_match = re.search(r'Contents\("([^"]+)"', expr_joined)
                if db_match:
                    new_item["sheet_table_location"] = db_match.group(1)
                table_match = re.search(r'Name\s*=\s*"([^"]+)"', expr_joined)
                if table_match:
                    new_item["sheet_table_name"] = table_match.group(1)
            else:
                new_item["partition_type"] = "unknown"

            enriched_data.append(new_item)

    # Save the result
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_data, f, indent=2)

    output_file



def convert_partition_json_to_excel(json_file='output/partition_summary_filteredA.json',
                                    excel_file='output/partition_summary_filteredA.xlsx'):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Normalize and flatten JSON
    flat_data = []
    for item in data:
        row = item.copy()
        # Convert list to multiline string for better readability in Excel
        if isinstance(row.get("partition_expression"), list):
            row["partition_expression"] = "\n".join(row["partition_expression"])
        flat_data.append(row)

    # Convert to DataFrame
    df = pd.DataFrame(flat_data)

    # Save to Excel
    df.to_excel(excel_file, index=False)
    print(f"✅ JSON converted to Excel: {excel_file}")
