# Spotify Data Pipeline Project

This project automates the extraction, processing, and loading of Spotify playlist data for global rankings using Google Cloud services and Terraform for infrastructure deployment.

## Project Overview

This pipeline:
1. Extracts Spotify data (Top 50 songs by country) via the Spotify API.
2. Processes the data and organizes it into separate tables (tracks, albums, artists, etc.).
3. Loads the processed data into Google Cloud Storage and BigQuery for analysis.

## Architecture Overview

A high-level view of the data flow:

![Spotify Data Pipeline Architecture](architecture_diagram.png) 

- **Compute Engine** runs the Airflow instance that triggers Spotify data extraction and processing tasks.
- **Google Cloud Storage** holds raw and processed data.
- **BigQuery** is used to store and analyze Spotify data in structured tables.

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

Clone the repository and naviguate to the project folder

```bash
git clone git@github.com:Leomendoza13/top_tracks_global_view.git
cd top_tracks_global_view
```

### Step 2: Create your new project on Google Cloud Platform Console

1. Create a [Google Cloud Platform Account](https://console.cloud.google.com/) if you haven’t already. New users get a free 3-month trial.

2. Go to your [console](https://console.cloud.google.com/) and create a new project using the "Create Project" button.

3. Go to **Compute Engine** tab and enable **Compute Engine API**. Repeat this for ****BigQuery API** to enable both services.

### Step 3: Configure GCloud CLI

1. Install [GCloud CLI](https://cloud.google.com/sdk/docs/install) if it’s not already installed.

2. Connect to your Google Cloud account and authenticate:

```bash
gcloud auth application-default login
```

This will generate a URL in your CLI, click on it, and log in to your Google Cloud account.

3. Set the project ID:

```bash
gcloud config set project [PROJECT_ID]
```

### Step 4: Configure Spotify Credentials

1. Create an account on the [Spotify API](https://developer.spotify.com/) if needed, and get your Spotify client credentials.

2. Open `config/spotify_api_id.json` and replace `"your_spotify_client_id"` and `"your_spotify_client_secret"` with your actual Spotify client credentials:

```bash
cat config/spotify_api_ids.json
```

Note: These credentials are sensitive and should be kept secure. Do not share or commit them publicly.

### Step 5: Configure Terraform Variables

1. Install [Terraform](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli) if it’s not already installed.

2. Create a `terraform.tfvars` file based on `example.tfvars`:

```bash
cp terraform/example.tfvars terraform/terraform.tfvars
```

3. Edit `terraform/terraform.tfvars` to add your specific values:

```
project_id       = "your-project-id"  
ssh_user         = "your-ssh-username"  
ssh_pub_key_path = "~/.ssh/id_rsa.pub"  
source_folder    = "../dags/"  
ids_path         = "../config/"
```

### Step 6: Deploy the Infrastructure

Navigate to the `terraform` folder, initialize Terraform and apply the configuration:

```bash
cd terraform  
terraform init  
terraform apply
```

Confirm the resources to be deployed. This command will set up:

- A Compute Engine instance to run the extraction and processing scripts.
- A Google Cloud Storage bucket for storing raw and processed data.
- BigQuery tables for storing and analyzing Spotify data.

### Step 7: Actions After `terraform apply`

After running the `terraform apply` command, your Google Cloud infrastructure is fully set up to support the extraction, processing, and loading of Spotify data. Here’s an overview of what happens next:

1. **Infrastructure Setup**: Once `terraform apply` completes, the following resources are created:
   - A Compute Engine instance is deployed to run the data extraction and transformation scripts.
   - A Google Cloud Storage bucket is configured to store both raw data extracted from Spotify and processed data.
   - BigQuery tables are created to organize and analyze Spotify data.

2. **Loading Scripts and Credentials**: Using the configurations specified in `terraform.tfvars`, the folders containing your Airflow DAGs (`dags/`) and your Spotify credentials (`config/`) are copied to the Compute Engine instance.

3. **Airflow Initialization**: The Compute Engine instance is configured to start Airflow and automatically load the DAGs in the `dags/` folder. Airflow is now set up to periodically trigger the Spotify data extraction and processing workflows.

4. **Pipeline Triggering**: Airflow initiates the pipeline automatically according to the schedule. After Airflow starts, it may take around 5 minutes for the pipeline to fully initialize and begin processing data.

5. **BigQuery Analysis**: Once the data is loaded, you can use BigQuery to query and analyze the Spotify data. For example, you can create visualizations or reports on music popularity trends by country and over time.

### **Step 8: ⚠️ DON'T FORGET TO `terraform destroy` WHEN IT IS DONE ⚠️**

```bash
terraform destroy
```

Running `terraform destroy` is essential after you’re done to prevent unnecessary costs. Google Cloud resources like Compute Engine instances and BigQuery storage incur charges as long as they’re active. By running `terraform destroy`, you ensure that all deployed resources are deleted, helping to avoid unexpected expenses.

### Usage

- **Extract and Load Data**: Use Airflow or a similar task orchestrator to trigger the DAGs in `dags/` for periodic data extraction and loading.
- **Analyze Data in BigQuery**: Use BigQuery SQL queries to analyze top Spotify tracks across countries. Here's a sample query to get started:

```sql
SELECT
    ft.track_name,
    da.artist_name,
    dp.playlist_name,
    ft.position
FROM
    `your_project_id.spotify_country_rankings.top_tracks` ft
JOIN `your_project_id.spotify_country_rankings.artists` da ON ft.artist_id = da.artist_id
JOIN `your_project_id.spotify_country_rankings.playlists` dp ON ft.playlist_id = dp.playlist_id
WHERE
    dp.playlist_name = 'Usa'
ORDER BY
    ft.position ASC
```

### Contributing

Feel free to submit issues or pull requests. For major changes, please discuss them via an issue to align with the project’s direction.

### License

This project is licensed under the MIT License.

### Author

This project was created and developed by me :) **Léo Mendoza**.

Feel free to reach out for questions, contributions, or feedback at [leo.mendoza@epita.com](mailto:leo.mendoza@epita.com).