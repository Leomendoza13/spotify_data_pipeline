resource "google_compute_instance" "airflow_instance" {
  name         = "airflow-instance"
  machine_type = "n2-standard-4"
  zone         = "europe-west9-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-focal-v20220712"
    }
  }

  service_account {
    email  = google_service_account.airflow_service_account.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.ssh_pub_key_path)}"
  }

  metadata_startup_script = file("${path.module}/scripts/startup-script.sh")
}

resource "null_resource" "create_dags_dir" {
  depends_on = [google_compute_instance.airflow_instance]

  provisioner "remote-exec" {
    inline = [
      "sudo mkdir -p /opt/airflow/dags /opt/airflow/config",
      "sudo chmod -R 755 /opt/airflow/dags /opt/airflow/config",
      "sudo chown -R ${var.ssh_user}:${var.ssh_user} /opt/airflow/dags /opt/airflow/config",
      "echo '{\"PROJECT_ID\": \"${var.project_id}\"}' | sudo tee /opt/airflow/config/project_id.json"
    ]

    connection {
      type        = "ssh"
      user        = var.ssh_user
      private_key = file(replace(var.ssh_pub_key_path, ".pub", ""))
      host        = google_compute_instance.airflow_instance.network_interface[0].access_config[0].nat_ip
    }
  }
}

resource "null_resource" "create_airflow_architecture" {
  depends_on = [null_resource.create_dags_dir]

  provisioner "file" {
    source      = var.ids_path
    destination = "/opt/airflow/config/"

    connection {
      type        = "ssh"
      user        = var.ssh_user
      private_key = file(replace(var.ssh_pub_key_path, ".pub", ""))
      host        = google_compute_instance.airflow_instance.network_interface[0].access_config[0].nat_ip
    }
  }

    provisioner "file" {
        source      = var.source_folder
        destination = "/opt/airflow/dags/"

        connection {
        type        = "ssh"
        user        = var.ssh_user
        private_key = file(replace(var.ssh_pub_key_path, ".pub", ""))
        host        = google_compute_instance.airflow_instance.network_interface[0].access_config[0].nat_ip
        }
    }

    provisioner "file" {
        source      = "../utils/"
        destination = "/opt/airflow/config/"

        connection {
        type        = "ssh"
        user        = var.ssh_user
        private_key = file(replace(var.ssh_pub_key_path, ".pub", ""))
        host        = google_compute_instance.airflow_instance.network_interface[0].access_config[0].nat_ip
        }
    }
}
