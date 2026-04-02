resource "aws_s3_bucket" "raw_data_lake" {
  bucket  = var.bucket_name 
  tags    = {
	Name          = "CapstoneBucket"
	Environment    = "Production"
  region          = var.aws_region
}
  lifecycle {
    prevent_destroy = true
  }
}

# # IAM Policy for Data Ingestion
# resource "aws_iam_policy" "ingestion_policy" {
#   name        = "SupplyChain360_Ingestion_Policy"
#   description = "Allows writing Parquet files to the dec_supplychain_bucket"

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
#         Effect   = "Allow"
#         Resource = [
#           "${aws_s3_bucket.raw_data_lake.arn}",
#           "${aws_s3_bucket.raw_data_lake.arn}/*"
#         ]
#       }
#     ]
#   })
# }


# iam policy
resource "aws_iam_policy" "s3_supplychain_policy" {
  name        = var.policy_name
  description = "Allows read, write, list access to created S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}/*",
          "arn:aws:s3:::${var.bucket_name}"
        ]
      }
    ]
  })
}

# The IAM User CreAtion
resource "aws_iam_user" "s3_user" {
  name = var.iam_user_name

  tags = {
    ManagedBy = "CapstoneTerraform"
  }
}


# Attach Policy to User
resource "aws_iam_user_policy_attachment" "attach" {
  user       = aws_iam_user.s3_user.name
  policy_arn = aws_iam_policy.s3_supplychain_policy.arn
}

# Create Access Key for the User
resource "aws_iam_access_key" "s3_user_key" {
  user = aws_iam_user.s3_user.name
}