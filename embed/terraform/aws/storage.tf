# The two shipped files reach the instance through a private bucket: EC2
# user data is capped at 16 KB and compose.yaml alone is larger. The
# instance role may read these two objects and nothing else in it.
resource "aws_s3_bucket" "this" {
  bucket_prefix = "${var.name}-"
  # destroy removes the objects and their old versions with the bucket.
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Untagged, both objects: S3 allows an object only 10 tags; the bucket
# carries local.tags.
#
# Not templatefile(): compose.yaml has its own ${...} interpolations.
resource "aws_s3_object" "compose_yaml" {
  bucket = aws_s3_bucket.this.id
  key    = "compose.yaml"
  source = "${path.module}/../../compose/compose.yaml"
  etag   = filemd5("${path.module}/../../compose/compose.yaml")

  # Versioning and encryption are in place before the first write.
  depends_on = [
    aws_s3_bucket_versioning.this,
    aws_s3_bucket_server_side_encryption_configuration.this,
  ]
}

resource "aws_s3_object" "dot_env" {
  bucket = aws_s3_bucket.this.id
  key    = ".env"
  source = "${path.module}/../../compose/.env"
  etag   = filemd5("${path.module}/../../compose/.env")

  depends_on = [
    aws_s3_bucket_versioning.this,
    aws_s3_bucket_server_side_encryption_configuration.this,
  ]
}
