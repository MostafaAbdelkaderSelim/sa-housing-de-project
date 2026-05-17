# infrastructure/main.tf
# ─────────────────────────────────────────────────────────────────────────────
# Infrastructure as Code — SA Housing Market DE Stack
# Provisions: S3 buckets, IAM roles, Glue catalog, Redshift Serverless
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "sa-housing-tf-state"
    key    = "prod/terraform.tfstate"
    region = "me-south-1"  # Bahrain — closest to KSA
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "SA-Housing-Market-DE"
      Environment = var.environment
      Owner       = "data-engineering"
      ManagedBy   = "terraform"
    }
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# Variables
# ══════════════════════════════════════════════════════════════════════════════
variable "aws_region"   { default = "me-south-1" }
variable "environment"  { default = "prod" }
variable "project_name" { default = "sa-housing" }

# ══════════════════════════════════════════════════════════════════════════════
# S3 Buckets — Data Lake (Bronze / Silver / Gold)
# ══════════════════════════════════════════════════════════════════════════════
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${var.environment}"
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-silver-${var.environment}"
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-gold-${var.environment}"
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "archive-old-raw"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# IAM — Glue Execution Role
# ══════════════════════════════════════════════════════════════════════════════
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "${var.project_name}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ══════════════════════════════════════════════════════════════════════════════
# Glue — Data Catalog & Crawler
# ══════════════════════════════════════════════════════════════════════════════
resource "aws_glue_catalog_database" "housing" {
  name        = "${var.project_name}_catalog"
  description = "SA Housing Market data catalog"
}

resource "aws_glue_crawler" "bronze_crawler" {
  database_name = aws_glue_catalog_database.housing.name
  name          = "${var.project_name}-bronze-crawler"
  role          = aws_iam_role.glue_role.arn
  schedule      = "cron(0 6 * * ? *)"

  s3_target {
    path = "s3://${aws_s3_bucket.bronze.bucket}/housing/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# ══════════════════════════════════════════════════════════════════════════════
# Glue ETL Job
# ══════════════════════════════════════════════════════════════════════════════
resource "aws_glue_job" "housing_etl" {
  name         = "${var.project_name}-etl-job"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.silver.bucket}/scripts/glue_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                = "python"
    "--enable-metrics"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--SOURCE_BUCKET"               = aws_s3_bucket.bronze.bucket
    "--TARGET_BUCKET"               = aws_s3_bucket.silver.bucket
    "--TempDir"                     = "s3://${aws_s3_bucket.silver.bucket}/tmp/"
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# Redshift Serverless — Data Warehouse
# ══════════════════════════════════════════════════════════════════════════════
resource "aws_redshiftserverless_namespace" "housing" {
  namespace_name      = "${var.project_name}-ns"
  db_name             = "housing_dw"
  admin_username      = "admin"
  admin_user_password = var.redshift_password
  iam_roles           = [aws_iam_role.glue_role.arn]
}

variable "redshift_password" {
  sensitive = true
  default   = "ChangeMe123!"
}

resource "aws_redshiftserverless_workgroup" "housing" {
  namespace_name = aws_redshiftserverless_namespace.housing.namespace_name
  workgroup_name = "${var.project_name}-wg"
  base_capacity  = 8   # RPUs — auto-scales
  publicly_accessible = false
}

# ══════════════════════════════════════════════════════════════════════════════
# Outputs
# ══════════════════════════════════════════════════════════════════════════════
output "bronze_bucket" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket" { value = aws_s3_bucket.silver.bucket }
output "gold_bucket"   { value = aws_s3_bucket.gold.bucket }
output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.housing.endpoint
}
