#!/bin/bash

# Directory containing your text files
RUNS_DIR="/fs/project/PAS1117/ricardo_vag_metag/data/runs"
# Output directory for SLURM scripts
SLURM_DIR="/fs/project/PAS1117/ricardo_vag_metag/scripts/jobs"
# Create the SLURM directory if it doesn't exist
mkdir -p $SLURM_DIR

# Loop through each txt file in the runs directory
for txt_file in ${RUNS_DIR}/*.txt; do
    # Extract the filename and remove the extension
    file_name=$(basename "$txt_file")
    study_name="${file_name%.txt}"
    
    # Create the SLURM job file
    slurm_file="${SLURM_DIR}/${study_name}.sh"
    
    cat > "$slurm_file" << EOF
#!/bin/bash

#SBATCH --account=PAS1117
#SBATCH --job-name=${SLURM_DIR}/${study_name}.sh
#SBATCH --time=24:00:00
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=24
#SBATCH --output=${SLURM_DIR}/${study_name}_%j.out
#SBATCH --error=${SLURM_DIR}/${study_name}_%j.err

# Load necessary modules
source ~/miniconda3/bin/activate

conda activate sra_tools

cd /fs/project/PAS1117/ricardo_vag_metag

# Run the download script
python /fs/project/PAS1117/ricardo_vag_metag/scripts/download_runs_file.py \\
    -r /fs/project/PAS1117/ricardo_vag_metag/data/runs/${file_name} \\
    -s ${study_name} \\
    -o /fs/scratch/Sullivan_Lab/Ricardo/vag_metag_db

EOF

    # Make the SLURM script executable
    chmod +x "$slurm_file"
    
    echo "Created SLURM job file: $slurm_file"
done

echo "All SLURM job files have been created."