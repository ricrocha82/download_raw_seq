# Read the data
out_dir = "/fs/project/PAS1117/ricardo_vag_metag/data/runs"
data <- read.csv("/fs/project/PAS1117/ricardo_vag_metag/data/SRR_runs_studies.csv", stringsAsFactors = FALSE)

# Get unique studies
studies <- unique(data$study)

# For each study, save accessions to a separate file
for (study in studies) {
  # Filter accessions for current study
  study_accessions <- data$accession[data$study == study]
  
  # Create filename
  filename <- file.path(out_dir, paste0(study, ".txt"))
  
  # Write accessions to file (one per line)
  writeLines(study_accessions, filename)
  
  # Print confirmation
  cat("Saved", length(study_accessions), "accessions to", filename, "\n")
}