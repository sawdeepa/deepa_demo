# pullDataFromApi.py Architecture Diagram

## High-Level Flow (Text Format)

```
┌─────────────────────────────────────────────────────────────────────┐
│                     LAMBDA HANDLER ENTRY POINT                      │
│                         lambda_handler()                            │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                    ┌────────────▼──────────────┐
                    │  Load Environment Vars    │
                    │  - BLS_SYNC_BUCKET        │
                    │  - BLS_SYNC_USER_AGENT    │
                    │  - BLS_SYNC_PREFIX        │
                    │  - DATAUSA_API_URL        │
                    └────────────┬──────────────┘
                                 │
                    ┌────────────▼──────────────┐
                    │   Validate Required       │
                    │   Environment Variables   │
                    └────────────┬──────────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         │                                               │
┌────────▼────────┐                            ┌────────▼────────┐
│   PART 1: BLS   │                            │  PART 2: DATAUSA│
│   Data Sync     │                            │  Population     │
└────────┬────────┘                            └────────┬────────┘
         │                                               │
         │                                               │
┌────────▼────────────────────────────────┐   ┌─────────▼──────────────────┐
│  1. discover_bls_files()                │   │  1. fetch_population_data()│
│     - GET BLS directory HTML            │   │     - GET DataUSA API      │
│     - Parse with regex                  │   │     - Parse JSON response  │
│     - Extract: name, url, timestamp     │   │                            │
└────────┬────────────────────────────────┘   └─────────┬──────────────────┘
         │                                               │
┌────────▼────────────────────────────────┐   ┌─────────▼──────────────────┐
│  2. List existing S3 files              │   │  2. save_population_to_s3()│
│     - Exclude deleted/ subfolder        │   │     - Generate timestamp   │
│     - Create set of S3 filenames        │   │     - Build S3 key         │
└────────┬────────────────────────────────┘   │     - Put object + metadata│
         │                                     └─────────┬──────────────────┘
┌────────▼────────────────────────────────┐            │
│  3. FOR EACH BLS file:                  │            │
│     ┌────────────────────────────────┐  │   ┌────────▼──────────────────┐
│     │ should_upload()?               │  │   │  3. Log Part 2 summary     │
│     │  - File new?      → Upload     │  │   │     - Success/Failure      │
│     │  - File updated?  → Upload     │  │   │     - Record count         │
│     │  - File current?  → Skip       │  │   │     - S3 key               │
│     └────────┬───────────────────────┘  │   └────────────────────────────┘
│              │                           │
│     ┌────────▼───────────────────────┐  │
│     │ stream_to_s3()                 │  │
│     │  - Direct HTTP → S3 upload     │  │
│     │  - Add source-url metadata     │  │
│     │  - Add timestamp metadata      │  │
│     └────────────────────────────────┘  │
└────────┬────────────────────────────────┘
         │
┌────────▼────────────────────────────────┐
│  4. Deletion Tracking:                  │
│     ┌────────────────────────────────┐  │
│     │ Find orphaned files:           │  │
│     │ (S3 files NOT in BLS listing)  │  │
│     └────────┬───────────────────────┘  │
│              │                           │
│     ┌────────▼───────────────────────┐  │
│     │ FOR EACH orphaned file:        │  │
│     │  - s3.copy_object() to         │  │
│     │    deleted/ subfolder          │  │
│     │  - s3.delete_object() from     │  │
│     │    active location             │  │
│     └────────────────────────────────┘  │
└────────┬────────────────────────────────┘
         │
┌────────▼────────────────────────────────┐
│  5. Log Part 1 summary:                 │
│     - Total files                       │
│     - Uploaded count                    │
│     - Skipped count                     │
│     - Moved count (to deleted/)         │
│     - Error count                       │
└────────┬────────────────────────────────┘
         │
         └───────────────────────┐
                                 │
                    ┌────────────▼──────────────┐
                    │  Determine Final Status   │
                    │  - Both success → 200     │
                    │  - Partial → 207          │
                    │  - Both failed → 500      │
                    └────────────┬──────────────┘
                                 │
                    ┌────────────▼──────────────┐
                    │  Return JSON Response     │
                    │  - statusCode             │
                    │  - body (part1 + part2)   │
                    │  - timestamp              │
                    └───────────────────────────┘
```

## Data Flow (End-to-End Pipeline)

```
┌──────────────────┐
│  BLS Website     │  https://download.bls.gov/pub/time.series/pr/
│  (12 Files)      │  
└────────┬─────────┘
         │ HTTP GET
         │ (discover_bls_files)
         ▼
┌────────────────────────────────┐
│  Lambda: pullDataFromApi       │
│  Part 1: BLS Sync              │
│  - Parse HTML directory        │
│  - Compare timestamps          │
│  - Stream to S3                │
│  - Track deletions             │
└────────┬───────────────┬───────┘
         │               │
         │               │ Orphaned files
         ▼               ▼
┌─────────────────┐   ┌──────────────────┐
│  S3: raw/pr/    │   │ S3: raw/pr/      │
│  (Active files) │   │ deleted/         │
│  - pr.data      │   │ (Archived)       │
│  - pr.series    │   └──────────────────┘
│  - pr.class     │
│  ... (12 files) │
└─────────────────┘


┌──────────────────┐
│  DataUSA API     │  https://honolulu-api.datausa.io/...
└────────┬─────────┘
         │ HTTP GET
         │ (fetch_population_data)
         ▼
┌────────────────────────────────┐
│  Lambda: pullDataFromApi       │
│  Part 2: Population Sync       │
│  - Fetch JSON data             │
│  - Add timestamp to filename   │
│  - Save to S3                  │
└────────┬───────────────────────┘
         │
         │ PUT Object
         ▼
┌─────────────────────────────────┐
│  S3: raw/datausa/population/    │
│  population_20241219_153045.json│
└────────┬────────────────────────┘
         │
         │ S3 Event Trigger
         │ (ObjectCreated:Put)
         ▼
┌─────────────────────────────────┐
│  Lambda: Report                 │
│  (Analytics Processing)         │
│  - Load BLS data                │
│  - Load population data         │
│  - Generate Q1, Q2, Q3 reports  │
└────────┬────────────────────────┘
         │
         │ PUT results
         ▼
┌─────────────────────────────────┐
│  S3: results/                   │
│  report_TIMESTAMP.json          │
└─────────────────────────────────┘
```

## Function Call Hierarchy

```
lambda_handler()
│
├─── sync_bls_to_s3()
│    │
│    ├─── discover_bls_files()
│    │    └─── urlopen() → parse HTML → extract files
│    │
│    ├─── FOR EACH file:
│    │    ├─── should_upload()
│    │    │    └─── s3.head_object() → compare timestamps
│    │    │
│    │    └─── stream_to_s3()
│    │         └─── urlopen() → s3.upload_fileobj()
│    │
│    └─── Deletion Tracking:
│         ├─── s3.list_objects_v2() → find S3 files
│         └─── FOR EACH orphaned:
│              ├─── s3.copy_object() → copy to deleted/
│              └─── s3.delete_object() → remove from active
│
└─── sync_datausa_to_s3()
     │
     ├─── fetch_population_data()
     │    └─── urlopen() → parse JSON
     │
     └─── save_population_to_s3()
          └─── s3.put_object()
```

## Deletion Tracking Logic

```
Step 1: Create set of current BLS files
┌─────────────────────────────────┐
│ BLS Files (from website):       │
│ {pr.data, pr.series, pr.class,  │
│  pr.contacts, ...}               │
└──────────────┬──────────────────┘
               │
               ▼
Step 2: List S3 files (exclude deleted/ subfolder)
┌─────────────────────────────────┐
│ S3 Files (in raw/pr/):          │
│ {pr.data, pr.series, pr.class,  │
│  pr.contacts, pr.old_file}      │
└──────────────┬──────────────────┘
               │
               ▼
Step 3: Find difference (S3 - BLS)
┌─────────────────────────────────┐
│ Orphaned Files:                 │
│ {pr.old_file}                   │
│                                 │
│ → File exists in S3 but NOT     │
│   in BLS listing (removed)      │
└──────────────┬──────────────────┘
               │
               ▼
Step 4: Move orphaned files
┌─────────────────────────────────┐
│ FOR EACH orphaned file:         │
│ 1. COPY: raw/pr/pr.old_file     │
│    TO:   raw/pr/deleted/...     │
│                                 │
│ 2. DELETE: raw/pr/pr.old_file   │
│                                 │
│ Result: File archived, not lost │
└─────────────────────────────────┘
```

## Upload Decision Flow (should_upload)

```
                ┌─────────────────┐
                │ Check if file   │
                │ exists in S3    │
                └────────┬────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
    ┌────▼────┐                    ┌────▼────┐
    │ NOT     │                    │ EXISTS  │
    │ FOUND   │                    │ IN S3   │
    │ (404)   │                    └────┬────┘
    └────┬────┘                         │
         │                               │
         ▼                        ┌──────▼──────┐
    ┌────────────┐                │ Has remote  │
    │ UPLOAD     │                │ timestamp?  │
    │ (new)      │                └──────┬──────┘
    └────────────┘                       │
                         ┌───────────────┴───────────────┐
                         │                               │
                    ┌────▼────┐                    ┌────▼────────┐
                    │ NO      │                    │ YES         │
                    │ timestamp│                   │ timestamp   │
                    └────┬────┘                    └────┬────────┘
                         │                              │
                         ▼                              │
                    ┌────────────┐              ┌───────▼────────┐
                    │ UPLOAD     │              │ Compare:       │
                    │ (no-       │              │ remote vs S3   │
                    │  timestamp)│              │ LastModified   │
                    └────────────┘              └───────┬────────┘
                                                        │
                                    ┌───────────────────┴───────────────┐
                                    │                                   │
                              ┌─────▼──────┐                     ┌─────▼──────┐
                              │ Remote     │                     │ Remote     │
                              │ > S3       │                     │ <= S3      │
                              │ (newer)    │                     │ (same/old) │
                              └─────┬──────┘                     └─────┬──────┘
                                    │                                   │
                                    ▼                                   ▼
                              ┌────────────┐                     ┌────────────┐
                              │ UPLOAD     │                     │ SKIP       │
                              │ (updated)  │                     │ (current)  │
                              └────────────┘                     └────────────┘
```

## Error Handling Strategy

```
┌──────────────────────────────────┐
│  Try Part 1 (BLS Sync)           │
└──────────┬───────────────────────┘
           │
     ┌─────┴─────┐
     │           │
┌────▼────┐  ┌──▼──────┐
│SUCCESS  │  │ ERROR   │
│Store    │  │ Log +   │
│result   │  │ Store   │
└────┬────┘  └──┬──────┘
     │          │
     └──────┬───┘
            │
┌───────────▼──────────────────────┐
│  Try Part 2 (DataUSA Sync)       │
└──────────┬───────────────────────┘
           │
     ┌─────┴─────┐
     │           │
┌────▼────┐  ┌──▼──────┐
│SUCCESS  │  │ ERROR   │
│Store    │  │ Log +   │
│result   │  │ Store   │
└────┬────┘  └──┬──────┘
     │          │
     └──────┬───┘
            │
┌───────────▼──────────────────────┐
│  Determine Final Status:         │
│  - Both success → 200             │
│  - One success  → 207 (partial)   │
│  - Both failed  → 500             │
└───────────────────────────────────┘
```

## Key Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `BLS_SYNC_BUCKET` | ✅ Yes | - | S3 bucket name |
| `BLS_SYNC_USER_AGENT` | ✅ Yes | - | HTTP User-Agent (email) |
| `BLS_SYNC_PREFIX` | No | `raw/pr/` | S3 prefix for BLS data |
| `BLS_SYNC_URL` | No | `https://download.bls.gov/...` | BLS directory URL |
| `DATAUSA_SYNC_PREFIX` | No | `raw/datausa/population/` | S3 prefix for population |
| `DATAUSA_API_URL` | No | `https://honolulu-api.datausa.io/...` | DataUSA API endpoint |

## Response Format

### Success (200)
```json
{
  "statusCode": 200,
  "body": {
    "status": "success",
    "timestamp": "2024-12-19T15:30:45Z",
    "request_id": "abc-123-def-456",
    "part1_bls": {
      "success": true,
      "total": 12,
      "uploaded": 2,
      "skipped": 10,
      "moved": 0,
      "errors": 0
    },
    "part2_population": {
      "success": true,
      "s3_key": "raw/datausa/population/population_20241219_153045.json",
      "record_count": 10,
      "api_url": "https://..."
    }
  }
}
```

### Partial Success (207)
```json
{
  "statusCode": 207,
  "body": {
    "status": "partial_success",
    "part1_bls": { "success": true, ... },
    "part2_population": { "success": false, "error": "API timeout" }
  }
}
```

### Failure (500)
```json
{
  "statusCode": 500,
  "body": {
    "status": "failed",
    "part1_bls": { "success": false, "error": "..." },
    "part2_population": { "success": false, "error": "..." }
  }
}
```
