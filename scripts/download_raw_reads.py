#!/usr/bin/env python3

import os
import subprocess
import argparse
import logging
from pathlib import Path
import shutil

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_sra_data(project_ids, output_folder, study_names=None, runs_only=False):
    """
    Download SRA data for a list of project IDs.
    
    Parameters:
    -----------
    project_ids : list
        List of SRA project IDs (e.g., ['PRJNA123456', 'PRJNA789012'])
    output_folder : str
        Path to the output folder where data will be stored
    study_names : list, optional
        List of study names corresponding to project_ids. If not provided,
        project IDs will be used as study names.
    runs_only : bool, optional
        If True, only generate run files without downloading data.
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    # If study_names is not provided, use project_ids as study_names
    if study_names is None:
        study_names = project_ids
    elif len(study_names) != len(project_ids):
        raise ValueError("Length of study_names must match length of project_ids")
    
    # Process each project ID with its corresponding study name
    for project, study_name in zip(project_ids, study_names):
        logger.info(f"Processing project: {project} (Study: {study_name})")
        
        # Create project_runs directory for this study
        project_runs_dir = output_path / study_name
        project_runs_dir.mkdir(exist_ok=True)
        
        # Get the run list from NCBI
        run_file = f"{project}_runs.txt"
        run_file_path = project_runs_dir / run_file
        
        try:
            if runs_only:
                # Save the full runinfo for reference
                logger.info(f"Generating run file for project: {project}")
                subprocess.run(
                    f"esearch -db sra -query {project} | efetch -format runinfo > {run_file_path}",
                    shell=True, check=True
                )
                logger.info(f"Generated run file for project: {project} at {run_file_path}")
            
            else:
                # Create project directory
                project_dir = project_runs_dir / project
                project_dir.mkdir(exist_ok=True)
                
                # Save the run accessions
                logger.info(f"Generating run accessions list for project: {project}")
                cmd = f"esearch -db sra -query {project} | efetch -format runinfo | cut -d ',' -f 1 | grep -v Run | grep -v '^$' > {run_file_path}"
                subprocess.run(cmd, shell=True, check=True)
                
                if not run_file_path.exists() or run_file_path.stat().st_size == 0:
                    logger.error(f"Failed to retrieve run information for {project}")
                    continue
                
                # Change to project directory for downloads
                os.chdir(project_dir)
                
                # Download the data
                logger.info(f"Downloading SRA files for project: {project}")
                try:
                    subprocess.run(
                        f"cat {run_file_path} | parallel -j0 prefetch {{}}",
                        shell=True, check=True
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error during prefetch for {project}: {e}")
                    continue
                
                # Move all .sra files out of their folders and into the working directory
                try:
                    logger.info("Moving SRA files to working directory")
                    subprocess.run(
                        "find . -name '*.sra' -exec mv {} . \\;",
                        shell=True, check=True
                    )
                    
                    # Delete all empty folders in the working directory
                    subprocess.run(
                        "find . -type d -empty -delete",
                        shell=True, check=True
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error during file organization: {e}")
                
                # Convert all .sra files into fastq files (split for paired-end)
                logger.info("Converting SRA files to FASTQ format")
                try:
                    subprocess.run(
                        "ls *.sra | parallel -j0 'fastq-dump --split-files --origfmt {}'",
                        shell=True, check=True
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error during fastq-dump conversion: {e}")
                
                # Compress all fastq files
                logger.info("Compressing FASTQ files")
                fastq_files = list(project_dir.glob("*.fastq"))
                if fastq_files:
                    try:
                        subprocess.run(
                            "pigz -1 *.fastq",
                            shell=True, check=True
                        )
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Error during gzip compression: {e}")
                else:
                    logger.warning("No FASTQ files found to compress")
                
                # Create directories for organization
                fastq_dir = project_dir / "fastq"
                sra_dir = project_dir / "sra"
                
                fastq_dir.mkdir(exist_ok=True)
                sra_dir.mkdir(exist_ok=True)
                
                # Move compressed FASTQ files
                fastq_gz_files = list(project_dir.glob("*.fastq.gz"))
                if fastq_gz_files:
                    try:
                        subprocess.run(
                            "mv *.fastq.gz fastq/",
                            shell=True, check=True
                        )
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Error moving compressed FASTQ files: {e}")
                
                # Move SRA files
                sra_files = list(project_dir.glob("*.sra"))
                if sra_files:
                    try:
                        subprocess.run(
                            "mv *.sra sra/",
                            shell=True, check=True
                        )
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Error moving SRA files: {e}")
                
                # Rename and move FASTQ files
                logger.info(f"Renaming and moving FASTQ files with study prefix: {study_name}")
                os.chdir(fastq_dir)
                for file_path in fastq_dir.glob("*.fastq.gz"):
                    file = file_path.name
                    new_name = f"{study_name}_{file}"
                    new_path = fastq_dir / new_name
                    
                    try:
                        # Rename file
                        file_path.rename(new_path)
                        logger.info(f"Renamed: {file} -> {new_name}")
                        
                        # Move to project runs directory
                        shutil.move(
                            str(new_path),
                            str(project_runs_dir / new_name)
                        )
                        logger.info(f"Moved: {new_name} -> {project_runs_dir}/{new_name}")
                    except (OSError, shutil.Error) as e:
                        logger.error(f"Error renaming/moving file {file}: {e}")
                
                # Clean up if fastq directory is empty
                if not any(fastq_dir.iterdir()):
                    logger.info("Removing empty fastq directory")
                    fastq_dir.rmdir()
                
                logger.info(f"Completed processing project: {project} from Study {study_name}")
        
        except Exception as e:
            logger.error(f"Error processing project {project}: {e}")

def read_csv_input(csv_file):
    """
    Read study names and project IDs from a CSV file.
    
    Parameters:
    -----------
    csv_file : str
        Path to the CSV file with study names and accession numbers
        
    Returns:
    --------
    tuple
        (study_names, project_ids)
    """
    import csv
    
    study_names = []
    project_ids = []
    
    try:
        with open(csv_file, 'r') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if len(row) < 2:
                    logger.warning(f"Skipping invalid row: {row}")
                    continue
                    
                study_name = row[0].strip()
                project_id = row[1].strip()
                
                if study_name and project_id:
                    study_names.append(study_name)
                    project_ids.append(project_id)
    
        if not study_names or not project_ids:
            raise ValueError("No valid data found in CSV file")
            
        logger.info(f"Read {len(project_ids)} projects from CSV file")
        return study_names, project_ids
        
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Download SRA data for multiple projects')
    
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-p', '--projects', nargs='+', help='List of SRA project IDs')
    input_group.add_argument('-c', '--csv', help='Path to CSV file with study names (col 1) and accession numbers (col 2)')
    
    parser.add_argument('-o', '--output', required=True, help='Output directory path')
    parser.add_argument('-s', '--studies', nargs='+', help='List of study names corresponding to project IDs (only used with -p)')
    parser.add_argument('-r', '--runs_only', action='store_true', help='Only generate run files without downloading data')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Determine input method (CSV or command line arguments)
    if args.csv:
        try:
            study_names, project_ids = read_csv_input(args.csv)
            download_sra_data(project_ids, args.output, study_names, args.runs_only)
        except Exception as e:
            logger.error(f"Failed to process CSV file: {e}")
            return 1
    else:
        download_sra_data(args.projects, args.output, args.studies, args.runs_only)
    
    return 0

if __name__ == "__main__":
    main()
