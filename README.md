[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/F15qHSfY)
# Event-Driven Image Processing - Lambda Architecture Project

# ![Pitt Panthers](https://upload.wikimedia.org/wikipedia/commons/4/44/Pitt_Panthers_wordmark.svg)
## Introduction to Cloud Computing - University of Pittsburgh

## Project Overview

This project demonstrates event-driven architecture using AWS services. You will build a serverless image processing pipeline that automatically processes images uploaded to S3 using AWS Lambda functions triggered via SNS topics.

When an image is uploaded to S3 with a specific prefix, an SNS notification triggers the corresponding Lambda function to process the image and upload the result back to S3 under a `/processed/` prefix.

## Architecture

```
S3 Bucket Upload (with prefix routing)
    └─> SNS Topic
        ├─> /resize/* → Resize Lambda → /processed/resize/
        ├─> /greyscale/* → Greyscale Lambda → /processed/greyscale/
        └─> /exif/* → EXIF Lambda → /processed/exif/
```

### Lambda Function Responsibilities

**Resize Lambda** (`/resize` prefix)
- Triggered when an image is uploaded to the `/resize/` prefix in the S3 bucket
- Downloads the original image from S3
- Resizes the image to 512x512 pixels
- Uploads the processed image to `/processed/resize/` in the same bucket

**Greyscale Lambda** (`/greyscale` prefix)
- Triggered when an image is uploaded to the `/greyscale/` prefix in the S3 bucket
- Downloads the original image from S3
- Converts the image to greyscale
- Uploads the processed image to `/processed/greyscale/` in the same bucket

**EXIF Lambda** (`/exif` prefix)
- Triggered when an image is uploaded to the `/exif/` prefix in the S3 bucket
- Downloads the original image from S3
- Extracts EXIF metadata from the image
- Uploads the extracted metadata to `/processed/exif/` in the same bucket

## Project Structure

```
.
├── .github/
│   └── workflows/
│       └── deploy-lambda.yml    # GitHub Actions workflow for ECR deployment
│       └── grading.yml          # manual grading workflow
├── lambdas/
│   ├── resize/
│   │   ├── Dockerfile
│   │   ├── handler.py
│   │   └── pyproject.toml
│   ├── greyscale/
│   │   ├── Dockerfile
│   │   ├── handler.py
│   │   └── pyproject.toml
│   └── exif/
│       ├── Dockerfile
│       ├── handler.py
│       └── pyproject.toml
└── README.md
```

## Prerequisites

- AWS Account
- GitHub Account

## What You Need to Implement

This project provides a skeleton with TODO sections. You must complete:

**1. Lambda Handler Code (3 files)**
- `lambdas/resize/handler.py` - add resize logic in the TODO section
- `lambdas/greyscale/handler.py` - add greyscale logic in the TODO section
- `lambdas/exif/handler.py` - add EXIF extraction logic in the TODO section
- refer to `local-dev.md` code implementations section for example code

**2. Dockerfiles (3 files)**
- `lambdas/resize/Dockerfile` - complete the Docker image build
- `lambdas/greyscale/Dockerfile` - complete the Docker image build
- `lambdas/exif/Dockerfile` - complete the Docker image build
- all three follow the same pattern: copy pyproject.toml, run uv sync, copy handler.py, set PYTHONPATH, set CMD

**3. GitHub Actions Configuration**
- update `AWS_LAMBDA_ROLE_ARN` in `.github/workflows/deploy.yml`
- add GitHub secrets: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
- the workflow will automatically build, push to ECR, and deploy Lambda functions

**4. AWS Infrastructure (follow setup guides in order)**
- create ECR repositories for each Lambda function
- create S3 bucket with input and processed prefixes
- create Lambda execution role with S3 and CloudWatch permissions
- create SNS topic and subscribe Lambda functions with filter policies
- configure S3 event notifications to publish to SNS

## Setup Guides

Follow these guides in order to set up your project:

1. **[Local Development Guide](./local-dev.md)** - Develop and test your Lambda functions locally
2. **[ECR Setup](./ecr-setup.md)** - Create container repositories for Lambda functions
3. **[S3 Setup](./s3-setup.md)** - Create and configure your S3 bucket with required prefixes
4. **[Lambda Setup](./lambda-setup.md)** - Create Lambda functions and execution roles
5. **[SNS Setup](./sns-setup.md)** - Configure SNS topic with filtered subscriptions
6. **[GitHub Actions Setup](./github-actions-setup)** - Automate deployment with GitHub Actions

## Testing

### End-to-End Test Script (Grading Script)

An automated test script is provided in the `hack/` directory. **This is the script used for grading.**

Run the test script with your bucket name:

```bash
cd hack
./test.sh <your pitt id>-assignment3
```

The script will upload test images, wait for processing, and verify all three Lambda functions work correctly.

### Manual Testing

You can also manually upload test images to your S3 bucket with the appropriate prefix:

```bash
# test resize
aws s3 cp test-image.jpg s3://<your-pitt-id>-assignment3/resize/test-image.jpg

# test greyscale
aws s3 cp test-image.jpg s3://<your-pitt-id>-assignment3/greyscale/test-image.jpg

# test exif
aws s3 cp test-image.jpg s3://<your-pitt-id>-assignment3/exif/test-image.jpg
```

Check CloudWatch Logs for each Lambda function to verify execution.

## Grading Requirements

This assignment is worth 15 points total:

**Lambda Implementation (6 points)**
- Resize Lambda correctly resizes images to 512x512 (2 points)
- Greyscale Lambda correctly converts to greyscale (2 points)
- EXIF Lambda correctly extracts metadata to JSON (2 points)

**Infrastructure Configuration (9 points)**
- ECR repositories created for all three Lambda functions (1 point)
- S3 bucket configured with correct prefixes (1 point)
- Lambda execution role with proper S3 and CloudWatch permissions (2 points)
- SNS topic created and configured (1 point)
- SNS subscriptions with correct filter policies routing to Lambdas (2 points)
- S3 event notifications configured to trigger SNS (1 point)
- Lambda functions deployed as container images via GitHub Actions (1 point)

The grading script will be run to verify your implementation. All three Lambda functions must successfully process images uploaded to their respective S3 prefixes.

## Submission

Submit the following:
1. Push all your code to the main branch
2. Please submit to canvas a .text file with the following:
s3-bucket.txt
```text
<your pitt id>-assignment3
```
