terraform {
    required_providers {
        google = {
        source  = "hashicorp/google"
        version = "~> 4.0"
        }
    }
}

provider "google" {
    credentials = file(var.google_credentials)
    project     = var.project_id
    region      = var.region
}

resource "google_storage_bucket" "bucket" {
    name          = "trademe-raw-data"
    location      = "US"
    force_destroy = true
}

resource "google_compute_instance" "trademe_viewer" {
    name         = "trademe-viewer-${var.environment}"
    machine_type = var.machine_type
    zone         = var.zone
    tags         = ["trademe-viewer", "vm", var.environment]

    boot_disk {
        initialize_params {
        image = var.vm_image
        size  = 100
        }
    }

    network_interface {
        network = "default"
        access_config {}
    }

    service_account {
        email  = var.service_account_email
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    }

    metadata_startup_script = templatefile("${path.module}/startup_script.sh", {
        docker_compose_version = var.docker_compose_version,
        github_username = var.github_username,
        github_token = var.github_token
    })

    metadata = {
        ssh-keys = "trademe:${file(var.ssh_public_key_path)}"
    }

    provisioner "remote-exec" {
        connection {
        type        = "ssh"
        user        = "trademe"
        private_key = file(var.ssh_private_key_path)
        host        = self.network_interface[0].access_config[0].nat_ip
        }

        inline = [
        "while [ ! -f /home/trademe/startup_script_completed ]; do sleep 10; done"
        ]
    }

    lifecycle {
        prevent_destroy = false
        ignore_changes  = [metadata["ssh-keys"]]
    }
}

# Variables
variable "project_id" {
    description = "GCP Project ID"
    type        = string
}

variable "region" {
    description = "GCP region"
    type        = string
    default     = "australia-southeast2"
}

variable "zone" {
    description = "GCP zone"
    type        = string
    default     = "australia-southeast2-a"
}

variable "google_credentials" {
    description = "Path to Google Cloud credentials file"
    type        = string
}

variable "service_account_email" {
    description = "Service Account Email"
    type        = string
}

variable "machine_type" {
    description = "VM machine type"
    type        = string
    default     = "e2-standard-4"
}

variable "vm_image" {
    description = "VM boot image"
    type        = string
    default     = "ubuntu-os-cloud/ubuntu-2204-lts"                                               
}

variable "environment" {
    description = "Deployment environment"
    type        = string
    default     = "dev"
}

variable "ssh_private_key_path" {
    description = "Path to SSH private key"
    type        = string
}

variable "ssh_public_key_path" {
    description = "Path to SSH public key"
    type        = string
}

variable "docker_compose_version" {
    description = "Docker Compose version"
    type        = string
    default     = "v2.20.3"
}

variable "github_token" {
    description = "GitHub Token"
    type        = string
}

variable "github_username" {
    description = "GitHub Username"
    type        = string
}

output "vm_public_ip" {
    value = google_compute_instance.trademe_viewer.network_interface[0].access_config[0].nat_ip
    description = "Public IP address of the VM instance"
}

output "bucket_name" {
    value = google_storage_bucket.bucket.name
}
