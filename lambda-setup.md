# Lambda Function Setup Guide

## Overview

AWS Lambda functions will process your images. You'll create three Lambda functions that are triggered by SNS notifications when images are uploaded to specific S3 prefixes. The Lambda functions will be deployed as container images to ECR via [Github Actions](.github/workflows/deploy-lambda.yml).

## Prerequisites

- ECR repositories created ([ECR Setup](./ecr-setup.md))
- S3 bucket created ([S3 Setup](./s3-setup.md))
- AWS CLI installed and configured

## Step 1: Create Lambda Execution Role

Lambda functions need an IAM role with permissions to access S3, write to CloudWatch Logs, and be invoked by SNS.

### Create Trust Policy

Create a file named `lambda-trust-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Create the IAM Role

```bash
aws iam create-role \
  --role-name lambda-image-processing-role \
  --assume-role-policy-document file://lambda-trust-policy.json
```

### Attach Policies

Attach the necessary managed policies:

```bash
# Policy for CloudWatch Logs
aws iam attach-role-policy \
  --role-name lambda-image-processing-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### Create Custom S3 Access Policy

Create a file named `lambda-s3-policy.json` (replace `<your-pitt-username>` with your actual username):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::<your pitt id>-assignment3/*"
    }
  ]
}
```

### Attach the S3 Policy

```bash
aws iam put-role-policy \
  --role-name lambda-image-processing-role \
  --policy-name S3AccessPolicy \
  --policy-document file://lambda-s3-policy.json
```

### Get the Role ARN

Save this ARN - you'll need it for creating Lambda functions and GitHub Actions:

```bash
aws iam get-role \
  --role-name lambda-image-processing-role \
  --query 'Role.Arn' \
  --output text
```

The output will be something like:
```
arn:aws:iam::ACCOUNT ID:role/lambda-image-processing-role
```

## Step 2: Wait for Initial Image Push

**Important:** Lambda functions that use container images require the image to exist in ECR before you can create the function. You can build and push manually, or let Github Actions deploy the image to ECR.

## Step 3: Create Lambda Functions

The GitHub Actions workflow will automatically create Lambda functions when you push code. However, you can also create them manually if needed.

Get your AWS Account ID:

```bash
aws sts get-caller-identity --query 'Account' --output text
```

Create the Lambda functions (replace `<FUNCTION NAME>`, `<AWS_ACCOUNT_ID>`, `<IMAGE TAG>`, `<AWS REGION>`,  and `<ROLE_ARN>`):

```bash
# create resize-lambda
aws lambda create-function \
  --function-name <FUNCTION NAME> \
  --package-type Image \
  --code ImageUri=<AWS_ACCOUNT_ID>.dkr.ecr.<AWS REGION>.amazonaws.com/resize-lambda:<IMAGE TAG> \
  --role <ROLE_ARN> \
  --timeout 30 \
  --memory-size 512 \
  --region us-east-1
```

## Step 4: Grant SNS Invoke Permission

After Lambda functions are created (either by GitHub Actions or manually), you need to grant SNS permission to invoke them.

**Note:** You'll need your SNS topic ARN from [SNS Setup](./sns-setup.md). If you haven't created it yet, complete that step first, then return here.

```bash
# Grant permission for resize-lambda
aws lambda add-permission \
  --function-name resize-lambda \
  --statement-id sns-invoke \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:image-processing-topic \
  --region us-east-1

# Grant permission for greyscale-lambda
aws lambda add-permission \
  --function-name greyscale-lambda \
  --statement-id sns-invoke \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:image-processing-topic \
  --region us-east-1

# Grant permission for exif-lambda
aws lambda add-permission \
  --function-name exif-lambda \
  --statement-id sns-invoke \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:image-processing-topic \
  --region us-east-1
```

## Step 5: Verify Lambda Functions

List your Lambda functions:

```bash
aws lambda list-functions \
  --query 'Functions[?starts_with(FunctionName, `resize`) || starts_with(FunctionName, `greyscale`) || starts_with(FunctionName, `exif`)].FunctionName' \
  --region us-east-1
```

You should see:
```json
[
    "resize-lambda",
    "greyscale-lambda",
    "exif-lambda"
]
```

## Cleanup (After Project Completion)

```bash
# Delete Lambda functions
aws lambda delete-function --function-name resize-lambda --region us-east-1
aws lambda delete-function --function-name greyscale-lambda --region us-east-1
aws lambda delete-function --function-name exif-lambda --region us-east-1

# Delete IAM role policies
aws iam delete-role-policy --role-name lambda-image-processing-role --policy-name S3AccessPolicy

# Detach managed policies
aws iam detach-role-policy \
  --role-name lambda-image-processing-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Delete IAM role
aws iam delete-role --role-name lambda-image-processing-role
```
