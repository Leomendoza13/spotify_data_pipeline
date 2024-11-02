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

variable "source_folder" {
  description = "Path to the local folder containing the files to upload"
  type        = string
}

variable "ids_path" {
  description = "Path of ids.json for SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET"
  type        = string
}