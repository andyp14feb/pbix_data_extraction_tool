import os
import hashlib
import subprocess
from pathlib import Path
from datetime import datetime
import shutil

def process_pbix_files(base_folder):
    """Main function to extract PBIX files"""
    if not base_folder:
        base_folder = "./input"
    log_progress(f'Entering log extraction... using base_folder: {base_folder}')


    # Process all files in base folder
    for root, _, files in os.walk(base_folder):
        for file in files:
            if file.lower().endswith(".pbix"):
                pbix_path = Path(root) / file
                output_folder = create_unique_output_folder(pbix_path.name, root)
                
                # Execute extraction
                try:
                    log_progress(f'going to extract  {pbix_path} to {output_folder}')
                    execute_extraction(pbix_path, output_folder)
                    print(f"Successfully extract {file}")
                except Exception as e:
                    log_error(f"Error extract {file}: {str(e)}")
                
                # # Execute export_data
                # try:
                #     log_progress(f'going to export_data  {pbix_path} to {output_folder}')
                #     execute_export_data(pbix_path, output_folder)
                #     print(f"Successfully export_data {file}")
                # except Exception as e:
                #     log_error(f"Error export_data {file}: {str(e)}")



def create_unique_output_folder(filename, source_path):
    """Create unique output folder with collision handling"""
    base_name = Path(filename).stem
    hash_suffix = hashlib.md5(source_path.encode()).hexdigest()[:8]
    output_folder = Path("output") / f"{base_name}_{hash_suffix}"
    output_folder.mkdir(exist_ok=True)
    return str(output_folder)

def execute_extraction(pbix_path, output_folder):
    """Execute pbi-tools.exe extraction command"""
    pbi_exe = Path(__file__).resolve().parent / "pbi-tools" / "pbi-tools.exe"
    command = [
        str(pbi_exe),
        "extract",
        str(pbix_path),
        "-extractFolder", output_folder,
        "-modelSerialization", "Tmdl"     
        # "-modelSerialization", "Raw"
    ]

    if not pbi_exe.exists():
        log_error(f"'pbi-tools.exe' not found at {pbi_exe}")
        raise FileNotFoundError(f"'pbi-tools.exe' not found at {pbi_exe}")
    
    log_progress(f"Executing command {command}")

    # Execute command
    result = subprocess.run(command, capture_output=True, text=True)
    
    # Check for errors
    if result.returncode != 0:
        raise Exception(f"Extraction failed: {result.stderr}")
    
    # Log output
    with open(Path(output_folder) / "extraction.log", "w") as f:
        f.write(result.stdout)
        if result.stderr:
            f.write("\n" + result.stderr)

    log_summary(pbix_path, output_folder,'extract')        

def execute_export_data(pbix_path, output_folder):
    """Export table data from PBIX using pbi-tools.exe export-data command"""
    pbi_exe = Path(__file__).resolve().parent / "pbi-tools" / "pbi-tools.exe"
    command = [
        str(pbi_exe),
        "export-data",
        "-pbixPath", str(pbix_path),
        "-outPath", str(output_folder)
    ]

    # Check if tool exists
    if not pbi_exe.exists():
        log_error(f"'pbi-tools.exe' not found at {pbi_exe}")
        raise FileNotFoundError(f"'pbi-tools.exe' not found at {pbi_exe}")
    
    log_progress(f"Executing export-data command: {command}")

    # Execute command
    result = subprocess.run(command, capture_output=True, text=True)
    
    # Check for errors
    if result.returncode != 0:
        raise Exception(f"Export-data failed: {result.stderr}")
    
    # Log output
    with open(Path(output_folder) / "processing_pbix.log", "w") as f:
        f.write(result.stdout)
        if result.stderr:
            f.write("\n" + result.stderr)

    log_summary(pbix_path, output_folder,'export-data')

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

if __name__ == "__main__":
    # Get base folder from user input
    clear_output_folder()
    base_folder = input("Enter the base folder to scan for PBIX files: ")
    log_progress(f'Scanning for PBIX files in {base_folder}')
    process_pbix_files(base_folder)
