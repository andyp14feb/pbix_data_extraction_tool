# PBIX Data Extraction Tool

## Background
My laziness levels are at an all-time high, so manual data extraction is absolutely off the table. I require a script that uses pbi-tools to handle a task given to me automatically: pulling information of what datasources being used in each .pbix files in a chosen folder, batch processing, and saving as JSON.

## Overview
The PBIX Data Extraction Tool is a Python script designed to extract data from Power BI (.pbix) files using the `pbi-tools` command-line utility. It processes PBIX files, exports table data, and generates structured output logs, summaries, and conversions (CSV/JSON) for further analysis.

## Key Features
- **Automated Extraction**: Extracts PBIX files into structured folders with detailed logs.
- **Data Export**: Exports table data using `pbi-tools` for further processing.
- **Output Conversion**: Converts extraction logs into CSV and JSON formats for easy analysis.
- **Error Handling**: Logs errors and progress during extraction and export processes.

## Usage Manual
1. **Prerequisites**:
   - Install Python 3.x.
   - Ensure `pbi-tools.exe` is in the `pbi-tools` directory (included in the project).
   - Place PBIX files in the `input/` folder or specify a custom base folder.

2. **Running the Script**:
   ```bash
   python pbix_extractor.py
   ```
   - The script will prompt for a base folder (default is `input/`).
   - It processes all `.pbix` files in the specified folder.

3. **Output**:
   - Extracted data and logs are saved in uniquely named folders under `output/`.
   - Summary files (`summary_log.csv`, `summary_log.json`) are generated in `output/`.

4. **Error Handling**:
   - Errors during extraction/export are logged in `extraction.log` and `processing_pbix.log` within each output folder.

## Tech Stack
- **Python**: Core language for scripting and automation.
- **pbi-tools**: Third-party CLI tool for PBIX file extraction and data export.
- **Standard Libraries**: `os`, `subprocess`, `pathlib`, `json` for file handling and process execution.

## Third-Party Tools
- **pbi-tools**: https://pbi.tools/ A command-line utility for interacting with Power BI files. It handles the actual extraction and export operations. Ensure it is properly installed and located in the `pbi-tools` directory.

## Notes
- The script uses `input()` to interactively request the base folder. For automation, modify the script to accept command-line arguments.
- Dependencies like `k_util.py` (logging, folder management) are included in the project.
