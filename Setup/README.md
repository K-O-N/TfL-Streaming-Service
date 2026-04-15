# Terraform and Google Cloud Platform

Terraform is an infrasture as code tool that enables you define both cloud and on-perm resources in human-readable configurations files that you can version, reuse, and share.

#### Requirements:
- GCP Account

#### Step To Run
- Create a GCP account
   - Under IAM & Admin, create a service account giving it the necessary permissions 
   - Under the service account created, create a key

- Create a new file under setup dir main.tf, using the terraform google cloud provider - link https://registry.terraform.io/providers/hashicorp/google/latest/docs, fill in the details on your project 
```
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.24.0"
    }
  }
}

provider "google" {
  # Configuration options
}
```
- Run ``` terraform init``` to initialise terraform and getthe provider; get the piece of code that terraform uses to talk to gcp.
- To create a a gcs bucket and bigquery dataset, use the link (https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket#example-usage---creating-a-private-bucket-in-standard-storage-in-the-eu-region-bucket-configured-as-static-website-and-cors-configurations) to get the file format and fill out the resource configurations 
- Run 
 ```terraform plan``` creates a preview of the changes to be applied against a remote state, allowing you to review the changes before applying them and  ```terraform apply``` to tell Terraform - applies the changes to the infrastructure 
 ``` terraform fmt ``` to format your configuration files so that they are consistent.