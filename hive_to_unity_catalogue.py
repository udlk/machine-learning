import os
import subprocess
import pandas as pd
import logging

# Configuration
input_file = "input.txt"  # Change this to your actual input file
output_folder = "executed_outputs"
merged_output_file = "merged_output.csv"
commands_log_file = "executed_commands.txt"
os.makedirs(output_folder, exist_ok=True)

# Setup logging
logging.basicConfig(
    filename="script.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
    # Read the input file
    with open(input_file, "r") as file:
        lines = file.readlines()
    
    # Save all commands to a file
    with open(commands_log_file, "w") as cmd_log:
        # Process each line
        for index, line in enumerate(lines):
            parts = line.strip().split(",")  # Assuming CSV format
            if len(parts) != 4:
                logging.warning(f"Skipping invalid line {index + 1}: {line}")
                continue
            
            case_number, ssn, begin_date, end_date = parts
            command = (
                f"python report.py -job_name='hist' "
                f"-case_number='{case_number}' -ssn='{ssn}' "
                f"-begin_date='{begin_date}' -end_date='{end_date}'"
            )
            
            # Write command to log file
            cmd_log.write(command + "\n")
            
            # Define output file
            output_file = os.path.join(output_folder, f"output_{index + 1}.csv")
            
            # Execute the command and save output
            logging.info(f"Executing: {command}")
            try:
                result = subprocess.run(command, shell=True, capture_output=True, text=True)
                with open(output_file, "w") as out_file:
                    out_file.write(result.stdout)
                
                if result.stderr:
                    logging.warning(f"Command stderr for line {index + 1}: {result.stderr}")
                    
            except Exception as e:
                logging.error(f"Error executing command for line {index + 1}: {e}")

    # Merge all CSV files into a single CSV with a single header
    csv_files = [os.path.join(output_folder, f) for f in os.listdir(output_folder) if f.endswith(".csv")]
    
    if csv_files:
        try:
            dataframes = []
            for file in csv_files:
                try:
                    df = pd.read_csv(file)
                    if not df.empty:
                        dataframes.append(df)
                except Exception as e:
                    logging.warning(f"Skipping file {file} due to read error: {e}")
            
            if dataframes:
                merged_df = pd.concat(dataframes, ignore_index=True)
                merged_df.to_csv(merged_output_file, index=False)
                logging.info(f"Merged CSV saved as {merged_output_file}")
            else:
                logging.warning("No valid CSV files found for merging.")
        except Exception as e:
            logging.error(f"Error merging CSV files: {e}")

except Exception as e:
    logging.critical(f"Script failed: {e}")
