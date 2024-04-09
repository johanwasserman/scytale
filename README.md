## Scytale Home Assignment

### Overview
This project involves building a data pipeline using PySpark to extract data from GitHub repositories, save it to JSON files in a data lake structure, and then process the data from the JSON files to save it in a data warehouse structure in Parquet format for analysis.

All data and logs have been committed to GitHub for convenience and to facilitate reviewing of this solution.

**Configuration File**
- Multiple organizations' GitHub data can be processed, and each organization may have one or more GitHub profiles.
- The `github_profiles.yml` file contains configuration details where organization keys and corresponding GitHub profiles are specified.
- Both the extract and transform scripts utilize this configuration file to process Pull Request data from GitHub.

**Extract Phase**
- The `pr_extract.py` module interacts with the GitHub API using PyGitHub to extract repository and pull request information from a GitHub profile.
- Extracted data is saved to a data lake-like path, partitioned by `<yyy-mm-dd>`.
- The folder structure below the partition path tracks `<org>` and `<profile>`.
- Each repository's data is stored in a JSON file.

**Transform Phase**
- The `pr_transform.py` module reads the extracted JSON files from the data lake and ensures that only the latest record for each repository is processed.
- Using PySpark, the data is transformed according to specifications and overwritten in a data warehouse-like structure with a folder per `<org>` and a Parquet dataset for each `<profile>`.

**Logging**
- Only errors are logged to the `logs` directory.

**Testing**
- Validation of the data in the Parquet dataset is performed using the `ReadParquetData.ipynb` Jupyter Notebook after running `python pr_extract.py` and `python pr_transform.py`.

### Environment Setup

- Python 3.11 virtual environment was used.
- Ensure Python and Spark are installed and configured.
- Install necessary Python libraries (`PyGithub`, `pyspark`, `PyYAML`) using `pip`.
- Additional libraries (`pandas`, `pyarrow`) are installed for the `ReadParquetData.ipynb` notebook using `pip`.

### Directory Structure

```
compliance_data_pipeline/
│
├── src/
│   ├── extract/
│   │   └── github/
│   │
│   ├── transform/
│   │   └── github/
│   │
│   └── airflow/
│       └── github/
│
├── logs/
│
├── data_lake/
│   └── github/
│       └── pull_requests/
│           └── <yyyy-mm-dd>/
│               └── <org>/
│                   └── <profile>/
│
└── data_warehouse/
    └── github/
        └── pull_requests/
            └── <org>/
                └── <profile>/
```

This directory structure organizes the project into separate modules for extraction (`src/extract/github/`), transformation (`src/transform/github/`), and Apache Airflow integration (`src/airflow/github/`). The `logs/` directory stores error logs, while data is partitioned in the `data_lake/` directory and organized in a data warehouse-like structure under `data_warehouse/`. This layout enhances clarity and facilitates maintenance and scalability of the data pipeline project.
