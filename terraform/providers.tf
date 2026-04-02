terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
  profile = "default"
  
}

# # Snowflake provider
# provider "snowflake" {
#   username = var.snowflake_user
#   password = var.snowflake_password
#   account  = var.snowflake_account
#   role     = "ACCOUNTADMIN"
# }