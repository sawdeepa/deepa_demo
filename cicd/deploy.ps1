# Deploy CloudFormation Stack for Rearc Data Pipeline - PRODUCTION
# This script will create/update all AWS resources automatically with _prod suffix

$StackName = "rearc-data-pipeline-prod"
$TemplateFile = "template-prod-clean.yaml"
$ParametersFile = "parameters.json"
$Region = "eu-north-1"

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Deploying Rearc Data Pipeline - PROD" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Stack Name:    $StackName" -ForegroundColor Yellow
Write-Host "Template:      $TemplateFile" -ForegroundColor Yellow
Write-Host "Parameters:    $ParametersFile" -ForegroundColor Yellow
Write-Host "Region:        $Region" -ForegroundColor Yellow
Write-Host ""

# Confirm deployment
$confirmation = Read-Host "Deploy stack? (y/n)"
if ($confirmation -ne 'y') {
    Write-Host "Deployment cancelled." -ForegroundColor Red
    exit
}

Write-Host ""
Write-Host "Deploying CloudFormation stack..." -ForegroundColor Green

# Deploy the stack
aws cloudformation deploy `
    --template-file $TemplateFile `
    --stack-name $StackName `
    --parameter-overrides file://$ParametersFile `
    --capabilities CAPABILITY_NAMED_IAM `
    --region $Region

# Check deployment status
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "======================================" -ForegroundColor Green
    Write-Host "Stack deployed successfully!" -ForegroundColor Green
    Write-Host "======================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Resources created:" -ForegroundColor Cyan
    Write-Host "  - Lambda: pullDatafromApi_prod" -ForegroundColor White
    Write-Host "  - Lambda: Report_prod" -ForegroundColor White
    Write-Host "  - S3 Bucket: rearc-deepa-demo-prod" -ForegroundColor White
    Write-Host "  - SQS Queue: SQSForReport_prod" -ForegroundColor White
    Write-Host "  - EventBridge Rule: Runat4pm_prod (daily 4 PM UTC)" -ForegroundColor White
    Write-Host "  - IAM Role: CopyFiles-role-imaykant-prod" -ForegroundColor White
    Write-Host ""
    Write-Host "View stack in AWS Console:" -ForegroundColor Cyan
    Write-Host "https://eu-north-1.console.aws.amazon.com/cloudformation/home?region=eu-north-1#/stacks" -ForegroundColor Blue
} else {
    Write-Host ""
    Write-Host "Deployment failed. Check the error above." -ForegroundColor Red
}
