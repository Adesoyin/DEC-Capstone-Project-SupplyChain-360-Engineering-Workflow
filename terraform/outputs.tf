output "bucket_created_id" {
  value = aws_s3_bucket.raw_data_lake.id
  description = "the id of the created bucket to host all parquet data"
}

output "s3_bucket_arn" {
  value = aws_s3_bucket.raw_data_lake.arn
  description = "the ARN of the created bucket to host all parquet data"
}

output "access_key_id" {
  description = "AWS Access Key ID"
  value       = aws_iam_access_key.s3_user_key.id
}

output "secret_access_key" {
  description = "AWS Secret Access Key"
  value       = aws_iam_access_key.s3_user_key.secret
  sensitive   = true
}

output "policy_arn" {
  description = "ARN of the created IAM policy"
  value       = aws_iam_policy.s3_supplychain_policy.arn
}

output "policy_name" {
  description = "Name of the created IAM policy"
  value       = aws_iam_policy.s3_supplychain_policy.id
}