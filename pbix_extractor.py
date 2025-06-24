import os
import hashlib
import subprocess
from pathlib import Path
from datetime import datetime

def extract_pbix_files(base_folder):
    """Main function to extract PBIX files"""
    if not base_folder:
        base_folder = "./input"
    log_extraction(f'Entering log extraction... using base_folder: {base_folder}')


    # Process all files in base folder
    for root, _, files in os.walk(base_folder):
        for file in files:
            if file.lower().endswith(".pbix"):
                pbix_path = Path(root) / file
                output_folder = create_unique_output_folder(pbix_path.name, root)
                
                # Execute extraction
                try:
                    log_extraction(f'going to extract  {pbix_path} to {output_folder}')
                    execute_extraction(pbix_path, output_folder)
                    print(f"Successfully processed {file}")
                except Exception as e:
                    log_error(f"Error processing {file}: {str(e)}")

def create_unique_output_folder(filename, source_path):
    """Create unique output folder with collision handling"""
    base_name = Path(filename).stem
    hash_suffix = hashlib.md5(source_path.encode()).hexdigest()[:8]
    output_folder = Path("output") / f"{base_name}_{hash_suffix}"
    output_folder.mkdir(exist_ok=True)
    return str(output_folder)

def execute_extraction(pbix_path, output_folder):
    """Execute pbi-tools.core.exe extraction command"""
    pbi_exe = Path(__file__).resolve().parent / "pbi-tools" / "pbi-tools.exe"
    command = [
        str(pbi_exe),
        "extract",
        str(pbix_path),
        "-extractFolder", output_folder,
        "-modelSerialization", "Raw"
    ]
    if not pbi_exe.exists():
        log_error(f"'pbi-tools.core.exe' not found at {pbi_exe}")
        raise FileNotFoundError(f"'pbi-tools.core.exe' not found at {pbi_exe}")
    
    log_extraction(f"Executing command {command}")

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

def get_current_datetime_str():
    """Returns current date and time as a string in format yyyymmdd_hhmm"""
    return datetime.now().strftime("%Y%m%d_%H%M")


def log_extraction(message):
    """Log extraction information to extraction log file"""
    with open("output/extraction_log.txt", "a") as f:
        f.write(f"{get_current_datetime_str()} : {message}\n")
        print(f"{get_current_datetime_str()} : {message}\n")
        
def log_error(message):
    """Log errors to error log file"""
    with open("output/error_log.txt", "a") as f:
        message=f"ERROR : {message}"
        f.write(f"{get_current_datetime_str()} : {message}\n")
        log_extraction(message)
        # print(f"{get_current_datetime_str()} : {message}\n")




if __name__ == "__main__":
    # Get base folder from user input
    base_folder = input("Enter the base folder to scan for PBIX files: ")
    log_extraction(f'Scanning for PBIX files in {base_folder}')
    extract_pbix_files(base_folder)
