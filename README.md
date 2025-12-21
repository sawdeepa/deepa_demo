# Rearc Data Quest - CloudFormation Deployment

## Overview
This project implements an automated data pipeline using AWS CloudFormation that:
- Fetches BLS (Bureau of Labor Statistics) data daily
- Retrieves population data from DataUSA API
- Generates analytics reports combining both datasets
- Runs automatically at 4 PM UTC daily via EventBridge

## Architecture

```
┌─────────────────┐
│  EventBridge    │  Daily 4 PM UTC trigger
│  Schedule Rule  │
└────────┬────────┘
         │
         v
┌─────────────────────────────────────┐
│  Lambda: pullDatafromApiProduction  │
│  - Syncs BLS data files             │
│  - Fetches DataUSA population data  │
└────────┬────────────────────────────┘
         │
         v
┌─────────────────────────────────┐
│  S3: rearc-deepa-demo-prod      │
│  - raw/pr/ (BLS data)           │
│  - raw/datausa/population/      │
└────────┬────────────────────────┘
         │ S3 Event Notification
         v
┌─────────────────────────────┐
│  SQS: SQSForReportProduction│
└────────┬────────────────────┘
         │ Event Source Mapping
         v
┌─────────────────────────────┐
│  Lambda: ReportProduction   │
│  - Generates analytics      │
│  - Combines BLS + Population│
└─────────────────────────────┘
```

## Prerequisites

### Required Tools
- **AWS CLI** (v2.x or later)
  - Windows: Already installed at `C:\Program Files\Amazon\AWSCLIV2\`
  - Verify: `aws --version`
  
- **AWS Account** with credentials configured
  - Run: `aws configure`
  - Provide: Access Key ID, Secret Access Key, Region (eu-north-1)

- **PowerShell** (for Windows deployment script)

### Required Files
1. Lambda deployment packages uploaded to S3:
   - `s3://rearc-deepa-demo/lambda-code/pullDataFromApi.zip` (4 KB)
   - `s3://rearc-deepa-demo/lambda-code/Report.zip` (2.9 KB)

## Project Structure

```
rearc/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── code/
│   ├── pullDataFromApi.py            # Combined ingestion Lambda (Part 1 + Part 2)
│   ├── Report.py                     # Analytics Lambda
│   └── pandas_data_analysis.ipynb    # Data exploration notebook
└── cicd/
    ├── template-prod-clean.yaml      # CloudFormation template
    ├── parameters.json               # Deployment parameters
    ├── deploy.ps1                    # PowerShell deployment script
    └── DEPLOYMENT_INSTRUCTIONS.md    # Detailed deployment guide
```

## Deployment Steps

### Step 1: Prepare Lambda Code (One-time setup)

If you need to update Lambda code:

```bash
# Package pullDataFromApi Lambda
cd code
zip pullDataFromApi.zip pullDataFromApi.py

# Package Report Lambda
zip Report.zip Report.py

# Upload to S3
aws s3 cp pullDataFromApi.zip s3://rearc-deepa-demo/lambda-code/ --region eu-north-1
aws s3 cp Report.zip s3://rearc-deepa-demo/lambda-code/ --region eu-north-1
```

### Step 2: Review Parameters

Edit `cicd/parameters.json` if needed:

```json
[
  {
    "ParameterKey": "LambdaFunctionPullDatafromApiCodeS3Bucket9vhkr",
    "ParameterValue": "rearc-deepa-demo"
  },
  {
    "ParameterKey": "LambdaFunctionPullDatafromApiCodeS3Keyk0Adq",
    "ParameterValue": "lambda-code/pullDataFromApi.zip"
  },
  {
    "ParameterKey": "LambdaFunctionReportCodeS3Bucketu9dAk",
    "ParameterValue": "rearc-deepa-demo"
  },
  {
    "ParameterKey": "LambdaFunctionReportCodeS3KeyUcJQB",
    "ParameterValue": "lambda-code/Report.zip"
  }
]
```

### Step 3: Deploy CloudFormation Stack

#### Option A: Using PowerShell Script (Recommended)

```powershell
cd cicd
.\deploy.ps1
```

When prompted, type `y` to confirm deployment.

#### Option B: Using AWS CLI Directly

```bash
cd cicd

aws cloudformation deploy \
  --template-file template-prod-clean.yaml \
  --stack-name rearc-data-pipeline-prod \
  --parameter-overrides file://parameters.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-north-1
```

#### Option C: Using AWS Console

1. Open [CloudFormation Console](https://eu-north-1.console.aws.amazon.com/cloudformation/)
2. Click **Create stack** → **With new resources**
3. Upload `template-prod-clean.yaml`
4. Stack name: `rearc-data-pipeline-prod`
5. Enter parameters (S3 bucket and keys)
6. Check: "I acknowledge that AWS CloudFormation might create IAM resources"
7. Click **Create stack**

### Step 4: Monitor Deployment

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name rearc-data-pipeline-prod \
  --region eu-north-1 \
  --query 'Stacks[0].StackStatus'

# Expected output: CREATE_COMPLETE or UPDATE_COMPLETE
```

**Deployment time:** ~2-3 minutes

### Step 5: Verify Resources

```bash
# List all created resources
aws cloudformation describe-stack-resources \
  --stack-name rearc-data-pipeline-prod \
  --region eu-north-1 \
  --query 'StackResources[].[LogicalResourceId,ResourceType,ResourceStatus]' \
  --output table
```

**Expected resources (9 total):**
- ✅ S3 Bucket: `rearc-deepa-demo-prod`
- ✅ Lambda: `pullDatafromApiProduction`
- ✅ Lambda: `ReportProduction`
- ✅ SQS Queue: `SQSForReportProduction`
- ✅ IAM Role: `CopyFiles-role-imaykant_prod`
- ✅ EventBridge Rule: `Runat4pm_prod`
- ✅ Lambda Event Source Mapping
- ✅ Lambda Permission
- ✅ SQS Queue Policy

## Testing

### Manual Test: Trigger Ingestion Lambda

```bash
# Invoke pullDatafromApiProduction manually
aws lambda invoke \
  --function-name pullDatafromApiProduction \
  --region eu-north-1 \
  response.json

# Check response
cat response.json
```

**Expected response:**
```json
{
  "statusCode": 200,
  "message": "Data sync completed successfully",
  "bls_sync": {
    "total_files": 15,
    "uploaded": 3,
    "skipped": 12,
    "success": true
  },
  "datausa_sync": {
    "success": true,
    "file_saved": "raw/datausa/population/population_2025-12-21.json"
  }
}
```

### Verify Data in S3

```bash
# List BLS data
aws s3 ls s3://rearc-deepa-demo-prod/raw/pr/ --region eu-north-1

# List population data
aws s3 ls s3://rearc-deepa-demo-prod/raw/datausa/population/ --region eu-north-1
```

### Check Lambda Logs

```bash
# Ingestion Lambda logs
aws logs tail /aws/lambda/pullDatafromApiProduction --follow --region eu-north-1

# Analytics Lambda logs
aws logs tail /aws/lambda/ReportProduction --follow --region eu-north-1
```

### Verify EventBridge Schedule

```bash
# Check schedule rule
aws events describe-rule \
  --name Runat4pm_prod \
  --region eu-north-1
```

## Updating the Stack

When you need to update Lambda code or configuration:

```bash
# 1. Update Lambda code (if needed)
cd code
zip pullDataFromApi.zip pullDataFromApi.py
aws s3 cp pullDataFromApi.zip s3://rearc-deepa-demo/lambda-code/ --region eu-north-1

# 2. Redeploy stack (updates existing resources)
cd ../cicd
aws cloudformation deploy \
  --template-file template-prod-clean.yaml \
  --stack-name rearc-data-pipeline-prod \
  --parameter-overrides file://parameters.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-north-1
```

**Note:** CloudFormation automatically detects changes and only updates modified resources.

## Cleaning Up

To delete all resources:

```bash
# 1. Empty S3 bucket (required before stack deletion)
aws s3 rm s3://rearc-deepa-demo-prod --recursive --region eu-north-1

# 2. Delete CloudFormation stack
aws cloudformation delete-stack \
  --stack-name rearc-data-pipeline-prod \
  --region eu-north-1

# 3. Wait for deletion to complete
aws cloudformation wait stack-delete-complete \
  --stack-name rearc-data-pipeline-prod \
  --region eu-north-1
```

## Troubleshooting

### Issue: "Resource already exists" error

**Cause:** Resources exist outside CloudFormation management (orphaned from failed deployments)

**Solution:**
```bash
# Delete orphaned resources manually
aws lambda delete-function --function-name pullDatafromApiProduction --region eu-north-1
aws lambda delete-function --function-name ReportProduction --region eu-north-1
aws sqs delete-queue --queue-url <queue-url> --region eu-north-1
aws s3 rb s3://rearc-deepa-demo-prod --force --region eu-north-1

# Then redeploy
cd cicd
.\deploy.ps1
```

### Issue: Lambda "Unable to import module" error

**Cause:** Handler configuration doesn't match filename in zip

**Solution:**
- Handler must be: `pullDataFromApi.lambda_handler` (not `lambda_function.lambda_handler`)
- Already fixed in `template-prod-clean.yaml`

### Issue: EventBridge "ScheduleExpression only supported on default bus"

**Cause:** Schedule rules only work on default event bus

**Solution:**
- Already fixed: Template uses `EventBusName: "default"`

### Issue: S3 bucket name contains underscore

**Cause:** S3 bucket names can only contain hyphens, not underscores

**Solution:**
- Already fixed: Bucket name uses `rearc-deepa-demo-prod` (hyphens)

## Configuration

### Lambda Environment Variables

**pullDatafromApiProduction:**
- `BLS_SYNC_BUCKET`: rearc-deepa-demo-prod
- `BLS_SYNC_URL`: https://download.bls.gov/pub/time.series/pr/
- `BLS_SYNC_PREFIX`: raw/pr/
- `BLS_SYNC_USER_AGENT`: babitadeepa@gmail.com
- `DATAUSA_API_URL`: DataUSA Honolulu API endpoint
- `DATAUSA_SYNC_PREFIX`: raw/datausa/population/

**ReportProduction:**
- `BUCKET_NAME`: rearc-deepa-demo-prod
- `BLS_DATA_KEY`: raw/pr/pr.data.0.Current
- `POPULATION_PREFIX`: raw/datausa/population/

### EventBridge Schedule

- **Schedule:** `cron(0 16 ? * * *)` - Daily at 4:00 PM UTC
- **Equivalent:** 10:00 PM IST / 11:00 AM EST / 8:00 AM PST

To change schedule, update `ScheduleExpression` in template.

## Key Features

### Resource Retention
- `UpdateReplacePolicy: "Retain"` - Keeps old resources during updates that require replacement
- `DeletionPolicy: "Retain"` - Prevents accidental deletion when stack is deleted
- **Protects:** S3 data, Lambda functions, SQS queues

### Security
- S3 bucket encryption: AES256
- Public access blocked on S3
- IAM least-privilege policies
- SQS server-side encryption enabled

### Data Flow
1. **EventBridge** triggers `pullDatafromApiProduction` at 4 PM UTC
2. **Lambda** fetches BLS data + population data → saves to S3
3. **S3** sends notification to SQS when `.json` file created in `raw/datausa/population/`
4. **SQS** triggers `ReportProduction` Lambda
5. **Lambda** generates analytics report combining both datasets

## License

This project is for educational purposes as part of the Rearc Data Quest.

## Contact

For questions or issues, please contact: babitadeepa@gmail.com
