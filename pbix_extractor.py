import os
import subprocess
from pathlib import Path
import json
import json
from k_util import create_unique_output_folder
from k_util import get_current_datetime_str
from k_util import log_progress
from k_util import log_error
from k_util import log_summary
from k_util import clear_output_folder
from k_util import convert_summary_log_to_csv
from k_util import convert_summary_log_to_json
from k_util import list_tmdl_files_to_json
from k_util import print_tmdl_contents
from k_util import extract_partitions_from_database_json

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


def execute_extraction(pbix_path, output_folder):
    """Execute pbi-tools.exe extraction command"""
    pbi_exe = Path(__file__).resolve().parent / "pbi-tools" / "pbi-tools.exe"
    command = [
        str(pbi_exe),
        "extract",
        str(pbix_path),
        "-extractFolder", output_folder,
        # "-modelSerialization", "Tmdl"     
        "-modelSerialization", "Raw"
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




if __name__ == "__main__":
    # Get base folder from user input
    # ===========================================
    # MVP1
    # ===========================================
    clear_output_folder()
    base_folder = input("Enter the base folder to scan for PBIX files: ")
    log_progress(f'Scanning for PBIX files in {base_folder}')
    process_pbix_files(base_folder)
    
    #===========================================
    # MVP1 step 02 listing all the
    #===========================================
    convert_summary_log_to_csv()
    convert_summary_log_to_json()
    # list_tmdl_files_to_json()
    # print_tmdl_contents()
    extract_partitions_from_database_json()
