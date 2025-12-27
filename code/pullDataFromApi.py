"""Combined Lambda handler for Part 1 & Part 2: BLS and DataUSA data ingestion.

Part 1: Streams BLS employment data files directly to S3
Part 2: Fetches DataUSA population data from API and saves to S3

Executes Part 1 first, then Part 2.
"""
import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import boto3
import botocore

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Default values (can be overridden by environment variables)
DEFAULT_BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
DEFAULT_DATAUSA_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
DEFAULT_TIMEOUT = 60


#==============================================================================
# PART 1: BLS DATA SYNC
#==============================================================================

def discover_bls_files(base_url: str, user_agent: str, timeout: int = DEFAULT_TIMEOUT) -> list[tuple[str, str, object]]:
    """Returns list of (filename, url, timestamp) from BLS directory listing."""
    logger.info(f"[Part 1] Fetching BLS directory listing from {base_url}")
    headers = {"User-Agent": user_agent, "Accept": "*/*"}
    req = Request(base_url, headers=headers, method="GET")
    
    with urlopen(req, timeout=timeout) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    
    # Parse Apache-style directory listing
    pattern = re.compile(
        r'(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s+(?:AM|PM))\s+\d+\s+<a\s+href="([^"]+)">([^<]+)</a>',
        re.IGNORECASE
    )
    
    files = []
    for match in pattern.finditer(html):
        date_str = match.group(1)
        time_str = match.group(2)
        href = match.group(3)
        name = match.group(4).strip()
        
        if href in ("../", "./") or href.endswith("/"):
            continue
        
        # Parse timestamp
        timestamp_str = f"{date_str} {time_str}"
        try:
            dt = datetime.strptime(timestamp_str, "%m/%d/%Y %I:%M %p")
            dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = None
        
        files.append((name, urljoin(base_url, href), dt))
    
    return files


def should_upload(remote_last_modified, s3_client, bucket: str, key: str) -> tuple[bool, str]:
    """Check if file needs uploading by comparing timestamps."""
    try:
        s3_obj = s3_client.head_object(Bucket=bucket, Key=key)
        s3_last_modified = s3_obj["LastModified"]
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return True, "new"
        raise
    
    if not remote_last_modified:
        return True, "no-timestamp"
    
    if remote_last_modified > s3_last_modified:
        return True, "updated"
    else:
        return False, "up-to-date"


def stream_to_s3(url: str, user_agent: str, s3_client, bucket: str, key: str, remote_last_modified=None, timeout: int = DEFAULT_TIMEOUT):
    """Stream file from URL directly to S3."""
    logger.info(f"[Part 1] Streaming {key} from BLS to S3")
    headers = {"User-Agent": user_agent, "Accept": "*/*"}
    req = Request(url, headers=headers, method="GET")
    
    with urlopen(req, timeout=timeout) as response:
        metadata = {"source-url": url}
        if remote_last_modified:
            metadata["source-last-modified"] = remote_last_modified.isoformat()
        
        s3_client.upload_fileobj(
            response,
            bucket,
            key,
            ExtraArgs={"Metadata": metadata},
        )
        logger.info(f"[Part 1] ✓ Successfully uploaded {key}")


def sync_bls_to_s3(bucket: str, user_agent: str, prefix: str, base_url: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """Part 1: Sync BLS employment data to S3."""
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    s3 = boto3.client("s3")
    
    logger.info("="*60)
    logger.info("PART 1: BLS Data Sync")
    logger.info(f"Source: {base_url}")
    logger.info(f"Target: s3://{bucket}/{prefix}")
    logger.info("="*60)
    
    # Discover files
    files = discover_bls_files(base_url, user_agent, timeout)
    logger.info(f"[Part 1] Found {len(files)} BLS files to process")
    
    # Get current S3 files for deletion tracking
    bls_file_names = {name for name, _, _ in files}
    try:
        s3_response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        s3_files = []
        if 'Contents' in s3_response:
            for obj in s3_response['Contents']:
                # Only track files in the direct prefix, not in deleted/ subfolder
                key = obj['Key']
                if key.startswith(f"{prefix}deleted/"):
                    continue
                filename = key.replace(prefix, "", 1)
                if filename and "/" not in filename:  # Direct files only
                    s3_files.append((filename, key))
        logger.info(f"[Part 1] Found {len(s3_files)} existing files in S3")
    except Exception as exc:
        logger.warning(f"[Part 1] Could not list S3 files for deletion tracking: {exc}")
        s3_files = []
    
    # Upload each file
    uploaded = 0
    skipped = 0
    errors = 0
    
    for idx, (name, url, remote_timestamp) in enumerate(files, 1):
        key = f"{prefix}{name}"
        logger.info(f"[Part 1] [{idx}/{len(files)}] Processing {name}")
        
        try:
            needs_upload, reason = should_upload(remote_timestamp, s3, bucket, key)
            
            if needs_upload:
                stream_to_s3(url, user_agent, s3, bucket, key, remote_timestamp, timeout)
                uploaded += 1
                logger.info(f"[Part 1]   ✓ UPLOADED: {name} ({reason})")
            else:
                skipped += 1
                logger.info(f"[Part 1]   - SKIPPED: {name} ({reason})")
        except Exception as exc:
            errors += 1
            logger.error(f"[Part 1]   ✗ ERROR: {name} - {exc}", exc_info=True)
    
    # Track deletions - move orphaned files to deleted/ folder
    moved = 0
    orphaned_files = [filename for filename, _ in s3_files if filename not in bls_file_names]
    
    if orphaned_files:
        logger.info(f"[Part 1] Found {len(orphaned_files)} orphaned files (removed from BLS)")
        deleted_prefix = f"{prefix}deleted/"
        
        for filename in orphaned_files:
            old_key = f"{prefix}{filename}"
            new_key = f"{deleted_prefix}{filename}"
            
            try:
                # Copy to deleted folder
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': old_key},
                    Key=new_key
                )
                # Delete original
                s3.delete_object(Bucket=bucket, Key=old_key)
                moved += 1
                logger.info(f"[Part 1]   ↔ MOVED: {filename} → deleted/")
            except Exception as exc:
                errors += 1
                logger.error(f"[Part 1]   ✗ ERROR moving {filename}: {exc}", exc_info=True)
    
    # Summary
    logger.info("="*60)
    logger.info("[Part 1] BLS Sync Summary:")
    logger.info(f"  Total files: {len(files)}")
    logger.info(f"  Uploaded:    {uploaded}")
    logger.info(f"  Skipped:     {skipped}")
    logger.info(f"  Moved:       {moved}")
    logger.info(f"  Errors:      {errors}")
    logger.info("="*60)
    
    return {
        "total": len(files),
        "uploaded": uploaded,
        "skipped": skipped,
        "moved": moved,
        "errors": errors,
        "success": errors == 0
    }


#==============================================================================
# PART 2: DATAUSA API SYNC
#==============================================================================

def fetch_population_data(api_url: str, user_agent: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """Fetch population data from DataUSA API."""
    logger.info(f"[Part 2] Fetching data from DataUSA API: {api_url}")
    
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json"
    }
    
    req = Request(api_url, headers=headers, method="GET")
    
    with urlopen(req, timeout=timeout) as response:
        data = json.loads(response.read().decode('utf-8'))
        logger.info(f"[Part 2] Successfully fetched data: {len(data.get('data', []))} records")
        return data


def save_population_to_s3(bucket: str, prefix: str, data: dict, api_url: str) -> str:
    """Save population JSON data to S3 with timestamp.
    
    Note: S3 event notification will automatically trigger analytics Lambda.
    """
    s3 = boto3.client("s3")
    
    # Create timestamped key
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{prefix.rstrip('/')}/population_{timestamp}.json"
    
    logger.info(f"[Part 2] Saving data to s3://{bucket}/{key}")
    
    # Convert to JSON string
    json_data = json.dumps(data, indent=2)
    
    # Upload to S3
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'source-url': api_url,
            'ingestion-timestamp': datetime.now(timezone.utc).isoformat(),
            'record-count': str(len(data.get('data', [])))
        }
    )
    
    logger.info(f"[Part 2] ✓ Successfully saved to S3: {key}")
    logger.info(f"[Part 2] S3 event notification will trigger analytics Lambda")
    
    return key


def sync_datausa_to_s3(bucket: str, user_agent: str, prefix: str, api_url: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """Part 2: Fetch DataUSA population data and save to S3."""
    logger.info("="*60)
    logger.info("PART 2: DataUSA Population Data Sync")
    logger.info(f"Source: {api_url}")
    logger.info(f"Target: s3://{bucket}/{prefix}")
    logger.info("="*60)
    
    try:
        # Fetch data from API
        data = fetch_population_data(api_url, user_agent, timeout)
        
        # Save to S3
        s3_key = save_population_to_s3(bucket, prefix, data, api_url)
        
        # Summary
        logger.info("="*60)
        logger.info("[Part 2] DataUSA Sync Summary:")
        logger.info(f"  Records:  {len(data.get('data', []))}")
        logger.info(f"  S3 Key:   {s3_key}")
        logger.info(f"  Success:  True")
        logger.info("="*60)
        
        return {
            "success": True,
            "s3_key": s3_key,
            "record_count": len(data.get('data', [])),
            "api_url": api_url
        }
        
    except Exception as e:
        logger.error(f"[Part 2] Error during DataUSA sync: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


#==============================================================================
# LAMBDA HANDLER - Combined Part 1 & 2
#==============================================================================

def lambda_handler(event, context):
    """Lambda entrypoint for combined Part 1 & Part 2 data ingestion.
    
    Environment Variables:
      Required:
        - BLS_SYNC_BUCKET: S3 bucket for data storage
        - BLS_SYNC_USER_AGENT: User agent for HTTP requests (email contact)
      Optional:
        - BLS_SYNC_PREFIX: S3 prefix for BLS data (default: raw/pr/)
        - BLS_SYNC_URL: BLS base URL (default: https://download.bls.gov/pub/time.series/pr/)
        - DATAUSA_SYNC_PREFIX: S3 prefix for population data (default: raw/datausa/population/)
        - DATAUSA_API_URL: DataUSA API URL (default: https://datausa.io/api/data?drilldowns=Nation&measures=Population)
    
    Returns:
        dict: Combined results from both Part 1 and Part 2
    """
    logger.info("="*70)
    logger.info("LAMBDA INVOKED: Combined Part 1 & Part 2 Data Ingestion")
    logger.info(f"Request ID: {context.aws_request_id if context else 'N/A'}")
    logger.info("="*70)
    
    # Get configuration from environment variables
    bucket = os.getenv("BLS_SYNC_BUCKET")
    user_agent = os.getenv("BLS_SYNC_USER_AGENT")
    bls_prefix = os.getenv("BLS_SYNC_PREFIX", "raw/pr/")
    bls_url = os.getenv("BLS_SYNC_URL", DEFAULT_BLS_URL)
    population_prefix = os.getenv("DATAUSA_SYNC_PREFIX", "raw/datausa/population/")
    datausa_url = os.getenv("DATAUSA_API_URL", DEFAULT_DATAUSA_URL)
    
    logger.info(f"Configuration:")
    logger.info(f"  Bucket:            {bucket}")
    logger.info(f"  User Agent:        {user_agent}")
    logger.info(f"  BLS URL:           {bls_url}")
    logger.info(f"  BLS Prefix:        {bls_prefix}")
    logger.info(f"  DataUSA URL:       {datausa_url}")
    logger.info(f"  Population Prefix: {population_prefix}")
    
    # Validate required variables
    if not bucket:
        logger.error("Missing required env var: BLS_SYNC_BUCKET")
        raise ValueError("Missing required env var: BLS_SYNC_BUCKET")
    if not user_agent:
        logger.error("Missing required env var: BLS_SYNC_USER_AGENT")
        raise ValueError("Missing required env var: BLS_SYNC_USER_AGENT")
    
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "request_id": context.aws_request_id if context else None,
    }
    
    # Execute Part 1: BLS Data Sync
    try:
        logger.info("\n" + "="*70)
        logger.info("EXECUTING PART 1: BLS Data Sync")
        logger.info("="*70)
        
        part1_result = sync_bls_to_s3(bucket, user_agent, bls_prefix, bls_url)
        results["part1_bls"] = part1_result
        
        logger.info(f"[Part 1] Status: {'SUCCESS' if part1_result['success'] else 'FAILED'}")
        
    except Exception as e:
        logger.error(f"[Part 1] Fatal error: {e}", exc_info=True)
        results["part1_bls"] = {
            "success": False,
            "error": str(e)
        }
    
    # Execute Part 2: DataUSA Population Sync
    try:
        logger.info("\n" + "="*70)
        logger.info("EXECUTING PART 2: DataUSA Population Sync")
        logger.info("="*70)
        
        part2_result = sync_datausa_to_s3(bucket, user_agent, population_prefix, datausa_url)
        results["part2_population"] = part2_result
        
        logger.info(f"[Part 2] Status: {'SUCCESS' if part2_result['success'] else 'FAILED'}")
        
    except Exception as e:
        logger.error(f"[Part 2] Fatal error: {e}", exc_info=True)
        results["part2_population"] = {
            "success": False,
            "error": str(e)
        }
    
    # Determine overall status
    part1_success = results.get("part1_bls", {}).get("success", False)
    part2_success = results.get("part2_population", {}).get("success", False)
    
    if part1_success and part2_success:
        overall_status = "success"
        status_code = 200
    elif part1_success or part2_success:
        overall_status = "partial_success"
        status_code = 207  # Multi-Status
    else:
        overall_status = "failed"
        status_code = 500
    
    results["status"] = overall_status
    

    logger.info("EXECUTION SUMMARY")
    logger.info("="*70)
    logger.info(f"Overall Status: {overall_status.upper()}")
    logger.info(f"Part 1 (BLS):   {'✓ SUCCESS' if part1_success else '✗ FAILED'}")
    logger.info(f"Part 2 (DataUSA): {'✓ SUCCESS' if part2_success else '✗ FAILED'}")
    logger.info("="*70)
    
    return {
        "statusCode": status_code,
        "body": json.dumps(results, indent=2)
    }
