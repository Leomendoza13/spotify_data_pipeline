# Spotify Data Pipeline Project

This project automates the extraction, processing, and loading of Spotify playlist data for global rankings using Google Cloud services and Terraform for infrastructure deployment.

## Project Overview

This pipeline:
1. Extracts Spotify data (Top 50 songs by country) via the Spotify API.
2. Processes the data and organizes it into separate tables (tracks, albums, artists, etc.).
3. Loads the processed data into Google Cloud Storage and BigQuery for analysis.

## Project Structure

```
    .
    ├── README.md
    ├── config
    │   ├── __init__.py
    │   ├── config.py
    │   └── spotify_api_ids.json
    ├── dags
    │   ├── extraction.py
    │   └── process_load.py
    ├── terraform
    │   ├── bigquery.tf
    │   ├── compute_instance.tf
    │   ├── example.tfvars
    │   ├── main.tf
    │   ├── provider.tf
    │   ├── scripts
    │   │   └── startup-script.sh
    │   ├── service_account.tf
    │   ├── storage.tf
    │   ├── terraform.tfstate
    │   ├── terraform.tfstate.backup
    │   ├── terraform.tfvars
    │   └── variables.tf
    └── utils
        ├── __init__.py
        ├── extraction_utils.py
        └── process_load_utils.py
```

## Prerequisites

- **Google Cloud Platform** with API access to Storage, BigQuery, and Compute Engine.
- **GCloud CLI** installed on your machine.
- **Spotify Developer Account** with access to client credentials.
- **Terraform** installed on your machine.

## Setup Instructions

### Step 1: Clone the Repository

```bash
    $ git clone git@github.com:Leomendoza13/top_tracks_global_view.git
    $ cd top_tracks_global_view
```

### Step 2: Create your new project on Google Cloud Platform Console

1. Create a [Google Cloud Platform Account](https://console.cloud.google.com/) if it not done yet. There is a free 3 months trial for new users.

2. Then go to your [console](https://console.cloud.google.com/) and create a new project on the upper left button.

3. After, that go to **Compute Engine** tab and enable **Compute Engine API**. Do the same for ****BigQuery API**.

### Step 3: Configure GCloud CLI

1. Install [GCloud CLI](https://cloud.google.com/sdk/docs/install) if it is not done yet.

2. Connect to your Google Cloud account:

```bash
    $ gcloud auth application-default login
```

This will give you a url in your CLI, you will just have to click on it and connect with your Google Cloud account.

Then set the project id with your actual existing project id you have set previously.

```bash
    $ gcloud config set project [PROJECT_ID]
```

### Step 4: Configure Spotify Credentials

1. If it is not the case yet, create an account on [spotify API](https://developer.spotify.com/) and get your Spotify client credentials.

2. Open `config/spotify_api_id.json` and replace `"your_spotify_client_id"` and `"your_spotify_client_secret"` with your actual Spotify client credentials.

```bash
    $ cat config/spotify_api_ids.json
    {
        "SPOTIFY_CLIENT_ID": "your_spotify_client_id",
        "SPOTIFY_CLIENT_SECRET": "your_spotify_client_secret"
    }
```

### Step 5: Configure Terraform Variables

1. Install [Terraform](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli) if it is not done yet.

2. Create a `terraform.tfvars` file based on `example.tfvars`:

```bash
    $ cp terraform/example.tfvars terraform/terraform.tfvars
```

3. Edit `terraform/terraform.tfvars` to add your specific values:

```
   project_id       = "your-project-id"  
   ssh_user         = "your-ssh-username"  
   ssh_pub_key_path = "/path/to/your/ssh-key.pub"  
   source_folder    = "../dags/"  
   ids_path         = "../config/"
```

### Step 6: Deploy the Infrastructure

Navigate to the `terraform` folder and initialize Terraform, then apply the configuration:

```bash
   $ cd terraform  
   $ terraform init  
   $ terraform apply
```

Confirm the resources to be deployed. This command will set up:

- A Google Cloud Storage bucket for storing raw and processed data.
- BigQuery tables for storing and analyzing Spotify data.
- A Compute Engine instance to run the extraction and processing scripts.

### Step 7: Run the Pipeline

### Usage

- **Extract and Load Data**: Use Airflow or a similar task orchestrator to trigger the DAGs in `dags/` for periodic data extraction and loading.
- **Analyze Data in BigQuery**: Use BigQuery SQL queries to analyze top Spotify tracks across countries.

### Contributing

Feel free to submit issues or pull requests. For major changes, please discuss them via an issue to align with the project’s direction.

### License

This project is licensed under the MIT License.