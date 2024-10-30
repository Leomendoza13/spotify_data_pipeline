variable "project_id" {
  description = "ID of project"
  type        = string
}

variable "ssh_user" {
  description = "SSH User"
  type        = string
}

variable "ssh_pub_key_path" {
  description = "SSH path key"
  type        = string
}

variable "dags_path" {
  description = "Local path of dags"
  type        = string
} #by default path = "./dags/extraction.py"

variable "ids_path" {
  description = "Path of ids.json for SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET"
  type        = string
}