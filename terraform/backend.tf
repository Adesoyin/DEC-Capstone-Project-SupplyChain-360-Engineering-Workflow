# terraform/backend

terraform {
  backend "s3" {
    bucket = "supplychain-statefile-bucket"
    key    = "env/terraform.tfstate"
    region = "eu-west-2"
    encrypt = true
    use_lockfile = true
  }
}
