# download raw seuqences from NCBI
Download fasta files using accession numbers directly from NCBI

Frist create the environment using:
```bash
mamba env create -f env_config.yaml
```

```bash
conda activate sra_tools
```

# Option 1
Using BioProject accession number

`-p` and `-s` can be list
```bash
python download_raw_reads.py -p <BioProject accession number> -o output/directory -s <name of the study>
```
If you want to generate only the metadata
```bash
python download_raw_reads.py -p <BioProject accession number> -o output/directory -s <name of the study> --runs_only
```

If you provide a `.csv` file, the code will automatically download the fasta files

The csv file should have two columns (study name, and accession number)
| study | accession |
| ------- |------- |
| XX_2020 | PRJNA1234 |
| YY_2021 | PRJNA1234 |
| ZZ_2022 | PRJNA1234 |
```bash
python download_raw_reads.py --csv dir/to/csv/file.csv -o output/directory
```

# Option 2
Using SRA accession numbers

input is a `.csv` file with two columns (study name, and accession number)
| accession |
| -------- |
| SRA1234    |
| SRA5678     |
| SRA8907    |
```bash
python download_runs_file.py --runs dir/to/csv/runs_file.csv -o output/directory -s XX_2020
```
