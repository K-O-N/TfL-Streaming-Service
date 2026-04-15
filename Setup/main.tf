terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.24.0"
    }
  }
}

provider "google" {
  project     = "tfl-system"
  region      = "EU"
  credentials = file("C:/Users/PC/Documents/Project/TfL_pipeline/TfL-Real-Time-Monitoring-System/Setup/keys/google_credentials.json")
}


resource "google_storage_bucket" "tfl-system-bucket" {
  name          = "tflsystem-bucket"
  location      = "EU"
  force_destroy = true
  
  # RULE 1: Clean up failed uploads after 1 day
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  # RULE 2: Delete successfully uploaded data after 90 days 
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}


resource "google_bigquery_dataset" "tfl-system-dataset" {
  dataset_id = "tfl_system_raw"
  location   = "EU"
}
