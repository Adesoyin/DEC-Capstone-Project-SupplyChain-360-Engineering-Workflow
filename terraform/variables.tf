variable "aws_region" {
    description = "The AWS region to deploy resources in"
    type        = string
    default     = "eu-west-2"
}

variable "platform_account_id" {
    description = "AWS Account ID for the platform account"
    type        = string
    default     = "886496686251"
}

variable "bucket_name" {
    description = "Name of the S3 bucket for the data lake"
    type        = string
    default     = "dec-supplychain-bucket"
}

variable "iam_user_name" {
  description = "Name of the IAM user"
  type        = string
  default     = "s3-supplychain"
}

variable "policy_name" {
  description = "Name of the IAM policy"
  type        = string
  default     = "s3-supplychain-policy"
}
