variable "project_id" {
  description = "ID of the Google Cloud project"
  type        = string
}

variable "ssh_user" {
  description = "SSH user for connecting to instances"
  type        = string
  default     = "default-ssh-user"
}

variable "ssh_pub_key_path" {
  description = "Path to the SSH public key"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "source_folder" {
  description = "Path to the local folder containing files to upload to the instance"
  type        = string
  default     = "../dags/"
}

variable "ids_path" {
  description = "Path to the ids.json file for SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET"
  type        = string
  default     = "../config/"
}