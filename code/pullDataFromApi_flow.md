# pullDataFromApi.py Flow Diagram

> **Note:** To view the Mermaid diagrams in VS Code, install the [Mermaid Preview](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid) extension or view on GitHub.

## Overview
Combined Lambda handler that executes Part 1 (BLS Data Sync) and Part 2 (DataUSA Population Sync) sequentially.

## Main Flow Diagram

```mermaid
flowchart TD
    Start([Lambda Invoked]) --> LoadEnv[Load Environment Variables]
    LoadEnv --> ValidateEnv{Required Env Vars<br/>Present?}
    
    ValidateEnv -->|No| Error1[Raise ValueError]
    ValidateEnv -->|Yes| InitResults[Initialize Results Dict]
    
    InitResults --> Part1Start[/Execute Part 1:<br/>BLS Data Sync/]
    
    %% Part 1 Flow
    Part1Start --> DiscoverBLS[discover_bls_files:<br/>Fetch HTML Directory Listing]
    DiscoverBLS --> ParseHTML[Parse with Regex:<br/>Extract filename, URL, timestamp]
    ParseHTML --> CreateBLSSet[Create Set of BLS Filenames]
    
    CreateBLSSet --> ListS3[List S3 Objects<br/>Exclude deleted/ subfolder]
    ListS3 --> LoopFiles{For Each<br/>BLS File}
    
    LoopFiles -->|Each File| CheckUpload[should_upload:<br/>Compare Timestamps]
    CheckUpload --> NeedsUpload{Needs<br/>Upload?}
    
    NeedsUpload -->|Yes - New| StreamNew[stream_to_s3:<br/>Upload with Metadata]
    NeedsUpload -->|Yes - Updated| StreamUpdate[stream_to_s3:<br/>Upload with Metadata]
    NeedsUpload -->|No - Up-to-date| Skip[Skip File]
    
    StreamNew --> IncrementUpload[uploaded++]
    StreamUpdate --> IncrementUpload
    Skip --> IncrementSkip[skipped++]
    
    IncrementUpload --> LoopFiles
    IncrementSkip --> LoopFiles
    
    LoopFiles -->|All Processed| FindOrphans[Find Orphaned Files:<br/>S3 files NOT in BLS set]
    
    FindOrphans --> HasOrphans{Orphaned<br/>Files?}
    HasOrphans -->|Yes| LoopOrphans{For Each<br/>Orphaned File}
    HasOrphans -->|No| Part1Summary
    
    LoopOrphans -->|Each Orphan| CopyToDeleted[s3.copy_object:<br/>Copy to deleted/ subfolder]
    CopyToDeleted --> DeleteOriginal[s3.delete_object:<br/>Remove from active]
    DeleteOriginal --> IncrementMoved[moved++]
    IncrementMoved --> LoopOrphans
    
    LoopOrphans -->|All Moved| Part1Summary[Log Part 1 Summary:<br/>total, uploaded, skipped, moved]
    Part1Summary --> Part1Result[Store Part 1 Result]
    
    %% Part 2 Flow
    Part1Result --> Part2Start[/Execute Part 2:<br/>DataUSA Population Sync/]
    Part2Start --> FetchAPI[fetch_population_data:<br/>GET request to DataUSA API]
    FetchAPI --> ParseJSON[Parse JSON Response]
    ParseJSON --> CreateTimestamp[Generate Timestamp:<br/>YYYYMMDD_HHMMSS]
    CreateTimestamp --> BuildKey[Build S3 Key:<br/>population_TIMESTAMP.json]
    BuildKey --> SaveS3[save_population_to_s3:<br/>Put Object with Metadata]
    SaveS3 --> Part2Summary[Log Part 2 Summary]
    Part2Summary --> Part2Result[Store Part 2 Result]
    
    %% Final Status
    Part2Result --> CheckBoth{Both Parts<br/>Successful?}
    CheckBoth -->|Yes| Success200[Status: success<br/>Code: 200]
    CheckBoth -->|One Failed| Partial207[Status: partial_success<br/>Code: 207]
    CheckBoth -->|Both Failed| Failed500[Status: failed<br/>Code: 500]
    
    Success200 --> Return[Return Response]
    Partial207 --> Return
    Failed500 --> Return
    Error1 --> Return
    
    Return --> End([End])
    
    style Part1Start fill:#e1f5ff
    style Part2Start fill:#fff4e1
    style Success200 fill:#d4edda
    style Partial207 fill:#fff3cd
    style Failed500 fill:#f8d7da
    style Error1 fill:#f8d7da
```

## Part 1: BLS Data Sync - Detailed Flow

```mermaid
flowchart LR
    A[BLS Website] -->|HTTP GET| B[discover_bls_files]
    B -->|Parse HTML| C[List of Files<br/>name, url, timestamp]
    C --> D{For Each File}
    D --> E[should_upload?]
    E -->|Check S3| F{File Exists?}
    F -->|No| G[Upload - New]
    F -->|Yes| H{Remote Newer?}
    H -->|Yes| I[Upload - Updated]
    H -->|No| J[Skip - Up-to-date]
    G --> K[stream_to_s3]
    I --> K
    K -->|Direct Stream| L[(S3 Bucket<br/>raw/pr/)]
    
    style A fill:#e3f2fd
    style L fill:#c8e6c9
```

## Part 2: DataUSA Population Sync - Detailed Flow

```mermaid
flowchart LR
    A[DataUSA API] -->|HTTP GET| B[fetch_population_data]
    B -->|Parse JSON| C[Population Data]
    C --> D[Generate Timestamp]
    D --> E[Create Filename:<br/>population_YYYYMMDD_HHMMSS.json]
    E --> F[save_population_to_s3]
    F -->|Put Object| G[(S3 Bucket<br/>raw/datausa/population/)]
    G -->|S3 Event Trigger| H[Analytics Lambda<br/>Report.py]
    
    style A fill:#fff9c4
    style G fill:#c8e6c9
    style H fill:#f3e5f5
```

## Deletion Tracking Logic

```mermaid
flowchart TD
    A[BLS Files Set] --> B[S3 Files List<br/>excluding deleted/]
    B --> C{Find Difference}
    C --> D[Orphaned Files =<br/>S3 files NOT in BLS set]
    D --> E{Has Orphans?}
    E -->|Yes| F[For Each Orphan]
    E -->|No| G[Continue]
    F --> H[Copy to deleted/ subfolder]
    H --> I[Delete from active location]
    I --> J[moved++]
    J --> F
    
    style D fill:#ffebee
    style H fill:#fff3e0
    style I fill:#ffcdd2
```

## Timestamp Comparison (should_upload)

```mermaid
flowchart TD
    A[should_upload] --> B{File in S3?}
    B -->|No - 404| C[Return True, 'new']
    B -->|Yes| D{Has Remote<br/>Timestamp?}
    D -->|No| E[Return True, 'no-timestamp']
    D -->|Yes| F{Remote > S3<br/>LastModified?}
    F -->|Yes| G[Return True, 'updated']
    F -->|No| H[Return False, 'up-to-date']
    
    style C fill:#c8e6c9
    style E fill:#fff9c4
    style G fill:#c8e6c9
    style H fill:#e1bee7
```

## Error Handling Flow

```mermaid
flowchart TD
    A[Lambda Handler] --> B{Try Part 1}
    B -->|Success| C[Store Part 1 Result]
    B -->|Exception| D[Log Error<br/>Store Error in Result]
    
    C --> E{Try Part 2}
    D --> E
    
    E -->|Success| F[Store Part 2 Result]
    E -->|Exception| G[Log Error<br/>Store Error in Result]
    
    F --> H[Determine Final Status]
    G --> H
    
    H --> I{Both Success?}
    I -->|Yes| J[200 - Success]
    I -->|Partial| K[207 - Partial Success]
    I -->|None| L[500 - Failed]
    
    style D fill:#ffcdd2
    style G fill:#ffcdd2
    style L fill:#f8d7da
    style K fill:#fff3cd
    style J fill:#d4edda
```

## Data Flow Summary

```mermaid
graph LR
    A[BLS Website<br/>12 Files] -->|Part 1| B[(S3<br/>raw/pr/)]
    C[DataUSA API] -->|Part 2| D[(S3<br/>raw/datausa/population/)]
    B -.->|If Orphaned| E[(S3<br/>raw/pr/deleted/)]
    D -->|S3 Event| F[Report Lambda]
    F -->|Analytics| G[(S3<br/>results/)]
    
    style A fill:#e3f2fd
    style C fill:#fff9c4
    style B fill:#c8e6c9
    style D fill:#c8e6c9
    style E fill:#ffebee
    style F fill:#f3e5f5
    style G fill:#c8e6c9
```

## Function Call Hierarchy

```mermaid
graph TD
    A[lambda_handler] --> B[sync_bls_to_s3]
    A --> C[sync_datausa_to_s3]
    
    B --> D[discover_bls_files]
    B --> E[should_upload]
    B --> F[stream_to_s3]
    B --> G[s3.copy_object]
    B --> H[s3.delete_object]
    
    C --> I[fetch_population_data]
    C --> J[save_population_to_s3]
    
    style A fill:#e1f5ff
    style B fill:#e1f5ff
    style C fill:#fff4e1
```

## Key Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `BLS_SYNC_BUCKET` | S3 bucket name | *Required* |
| `BLS_SYNC_USER_AGENT` | HTTP User-Agent header | *Required* |
| `BLS_SYNC_PREFIX` | S3 prefix for BLS data | `raw/pr/` |
| `BLS_SYNC_URL` | BLS directory URL | `https://download.bls.gov/pub/time.series/pr/` |
| `DATAUSA_SYNC_PREFIX` | S3 prefix for population data | `raw/datausa/population/` |
| `DATAUSA_API_URL` | DataUSA API endpoint | `https://honolulu-api.datausa.io/...` |

## Execution Summary

### Part 1 Output
- `total`: Number of BLS files discovered
- `uploaded`: Files uploaded (new or updated)
- `skipped`: Files already up-to-date
- `moved`: Orphaned files moved to deleted/
- `errors`: Number of errors encountered

### Part 2 Output
- `success`: Boolean indicating success
- `s3_key`: S3 key where data was saved
- `record_count`: Number of population records
- `api_url`: Source API URL

### Final Response
- `statusCode`: 200 (success), 207 (partial), 500 (failed)
- `body`: JSON with both part results and overall status
