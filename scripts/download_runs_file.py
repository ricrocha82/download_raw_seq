#!/usr/bin/env python3

import os
import subprocess
import argparse
from pathlib import Path
import shutil
import sys


def download_sra_data(runs_file, output_folder, study_name):
    """
    Download SRA data for a list of runs from a file.
    
    Parameters:
    -----------
    runs_file : str
        Path to file containing SRA run accessions (one per line)
    output_folder : str
        Path to the output folder where data will be stored
    study_name : str
        Name of the study for file naming
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_folder).resolve()
    print(f"Creating output directory: {output_path}")
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create study directory
    study_dir = output_path / study_name
    print(f"Creating study directory: {study_dir}")
    study_dir.mkdir(exist_ok=True)
    
    # Create temporary working directory
    temp_dir = study_dir / "temp"
    print(f"Creating temporary directory: {temp_dir}")
    temp_dir.mkdir(exist_ok=True)
    
    # Create fastq and sra directories
    fastq_dir = study_dir / "fastq"
    sra_dir = study_dir / "sra"
    
    print(f"Creating fastq directory: {fastq_dir}")
    fastq_dir.mkdir(exist_ok=True)
    
    print(f"Creating sra directory: {sra_dir}")
    sra_dir.mkdir(exist_ok=True)
    
    # Save current directory to return to it later
    original_dir = os.getcwd()
    
    try:
        # Change to temp directory for downloads
        os.chdir(temp_dir)
        print(f"Changed working directory to: {temp_dir}")
        
        # Check if runs file exists and is readable
        run_file_path = Path(runs_file).resolve()
        if not run_file_path.exists():
            raise FileNotFoundError(f"Runs file not found: {run_file_path}")
        
        print(f"Starting download of SRA data from: {run_file_path}")
        
        # Download the data
        cmd = f"cat {run_file_path} | parallel -j0 prefetch {{}}"
        print(f"Running command: {cmd}")
        subprocess.run(cmd, shell=True, check=True)
        
        # Handle SRA files more safely
        sra_files = list(temp_dir.glob("**/*.sra"))
        if not sra_files:
            print("No .sra files found after prefetch")
        else:
            print(f"Found {len(sra_files)} .sra files")
            
            # Move all .sra files to the current directory (with error handling)
            for sra_file in sra_files:
                try:
                    target = temp_dir / sra_file.name
                    print(f"Moving {sra_file} to {target}")
                    shutil.move(str(sra_file), str(target))
                except (shutil.Error, OSError) as e:
                    print(f"Error moving {sra_file}: {e}", file=sys.stderr)
        
        # Convert all .sra files into fastq files (split for paired-end)
        sra_files = list(temp_dir.glob("*.sra"))
        if sra_files:
            print(f"Converting {len(sra_files)} .sra files to fastq")
            cmd = "ls *.sra | parallel -j0 fastq-dump --split-files --origfmt {}"
            subprocess.run(cmd, shell=True, check=True)
            
            # Compress all fastq files
            fastq_files = list(temp_dir.glob("*.fastq"))
            if fastq_files:
                print(f"Compressing {len(fastq_files)} fastq files")
                cmd = "pigz -1 *.fastq"
                subprocess.run(cmd, shell=True, check=True)
            else:
                print("No fastq files found after conversion")
        else:
            print("No .sra files found for conversion")
        
        # Move and rename fastq files
        fastq_gz_files = list(temp_dir.glob("*.fastq.gz"))
        if fastq_gz_files:
            print(f"Moving and renaming {len(fastq_gz_files)} fastq.gz files")
            for file in fastq_gz_files:
                new_name = f"{study_name}_{file.name}"
                target = fastq_dir / new_name
                try:
                    print(f"Moving {file} to {target}")
                    shutil.move(str(file), str(target))
                except (shutil.Error, OSError) as e:
                    print(f"Error moving {file}: {e}", file=sys.stderr)
        else:
            print("No fastq.gz files found to move")
        
        # Move sra files
        sra_files = list(temp_dir.glob("*.sra"))
        if sra_files:
            print(f"Moving {len(sra_files)} .sra files to sra directory")
            for file in sra_files:
                target = sra_dir / file.name
                try:
                    print(f"Moving {file} to {target}")
                    shutil.move(str(file), str(target))
                except (shutil.Error, OSError) as e:
                    print(f"Error moving {file}: {e}", file=sys.stderr)
        else:
            print("No .sra files found to move")
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}: {e.cmd}", file=sys.stderr)
        print(e.output if hasattr(e, 'output') else "No output captured", file=sys.stderr)
        raise
    except Exception as e:
        print(f"Error during processing: {e}", file=sys.stderr)
        raise
    finally:
        # Return to original directory
        os.chdir(original_dir)
        print(f"Changed back to original directory: {original_dir}")
        
        # Clean up temporary directory if it exists
        if temp_dir.exists():
            try:
                print(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
            except OSError as e:
                print(f"Error removing temporary directory: {e}", file=sys.stderr)
    
    print(f"Completed processing SRA data for study: {study_name}")


def main():
    parser = argparse.ArgumentParser(description='Download SRA data for a study')
    parser.add_argument('-r', '--runs', required=True, help='File with SRA run accessions (one per line)')
    parser.add_argument('-o', '--output', required=True, help='Output directory path')
    parser.add_argument('-s', '--study', required=True, help='Study name for file naming')
    
    args = parser.parse_args()
    
    download_sra_data(args.runs, args.output, args.study)


if __name__ == "__main__":
    main()