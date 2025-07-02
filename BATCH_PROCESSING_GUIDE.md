# OpenAI Batch Processing Guide

## Overview

The AI Music Metadata Project now supports **OpenAI Batch Processing** for significant cost savings and improved efficiency when processing large collections of CDs.

## Benefits of Batch Processing

- **50% Cost Savings**
- **Higher Rate Limits**
- **Same Quality**
- **Reduced Monitoring**

## How It Works

### Automatic Mode (Recommended)
By default, the system automatically chooses the best processing method:
- **≤10 items**: Uses real-time processing for faster results
- **>10 items**: Uses batch processing for cost savings

### Manual Control
Set the `USE_BATCH_PROCESSING` environment variable:

```bash
# Force batch processing for all sizes
export USE_BATCH_PROCESSING=true

# Force real-time processing for all sizes  
export USE_BATCH_PROCESSING=false

# Use automatic mode (default)
export USE_BATCH_PROCESSING=auto
```

## Which Steps Use Batch Processing

Only steps that make OpenAI API calls support batch processing:

- **Step 1**: ✅ CD image analysis (vision API)
- **Step 1.5**: ❌ Data cleaning (no API calls)
- **Step 2**: ❌ OCLC search (different API)
- **Step 3**: ✅ OCLC analysis (text API)
- **Step 4**: ❌ Verification (no API calls)
- **Step 5**: ❌ Final output (no API calls)

## Processing Times

### Real-Time Processing
- **Immediate**: Results available as soon as each API call completes
- **Interactive**: See progress and results in real-time
- **Best for**: Small batches (≤10 items) or urgent processing

### Batch Processing
- **Delayed**: Takes up to 24 hours (usually much faster)
- **Hands-off**: Submit job and check back later
- **Best for**: Large batches (>10 items) or cost-sensitive processing

## Usage Examples

### Using the Run Script
The main run script automatically handles batch processing:

```bash
# Automatic mode (recommended)
python run_cd_processing.py

# Force batch processing
USE_BATCH_PROCESSING=true python run_cd_processing.py

# Force real-time processing  
USE_BATCH_PROCESSING=false python run_cd_processing.py
```

### Running Individual Steps
You can also run steps individually with batch processing:

```bash
# Step 1 with batch processing
USE_BATCH_PROCESSING=true python ai-music-step-1-cd.py

# Step 3 with batch processing
USE_BATCH_PROCESSING=true python ai-music-step-3-cd.py
```

## Monitoring Batch Jobs

When using batch processing, you'll see:

1. **Job Submission**: Confirmation that your batch was submitted
2. **Batch ID**: Unique identifier for tracking your job
3. **Status Updates**: Progress updates every 5 minutes
4. **Completion**: Results processed and saved to Excel

Example output:
```
📤 Uploading batch file with 100 requests...
✅ Batch job submitted successfully!
   Batch ID: batch_abc123xyz
   Requests: 100
   Status: validating

⏳ Waiting for batch completion (ID: batch_abc123xyz)
   Max wait time: 24 hours
   Check interval: 5 minutes

🔄 Batch Status: in_progress
   Progress: 45/100 completed, 0 failed

✅ Batch completed successfully!
📥 Downloading batch results...
✅ Retrieved 100 batch results
```

## Error Handling

Batch processing includes robust error handling:
- Failed individual requests are logged and marked
- Successful requests are processed normally
- Partial batch failures don't stop the entire workflow
- Detailed error logs are created for troubleshooting

## Best Practices

1. **Use Automatic Mode**: Let the system choose based on batch size
2. **Monitor Large Jobs**: Check status periodically for large batches
3. **Plan Timing**: Allow extra time for batch processing
4. **Check Logs**: Review batch logs for any processing issues
5. **Test Small First**: Try with a small batch before processing hundreds

## Troubleshooting

### Batch Job Stuck
If a batch job seems stuck:
1. Check the logs for error messages
2. Verify your OpenAI API quota isn't exceeded
3. Try submitting a smaller test batch

### Mixed Results
If some items process and others fail:
1. Check the individual response logs
2. Look for data quality issues in failed items
3. Failed items can be reprocessed individually

### Cost Concerns
To minimize costs:
1. Use automatic mode for optimal cost/speed balance
2. Clean your image data before processing
3. Remove duplicate or invalid images

## Environment Setup

Ensure these environment variables are set:

```bash
# Required for all processing
export OPENAI_API_KEY="your-openai-api-key"
export OCLC_CLIENT_ID="your-oclc-client-id"  
export OCLC_SECRET="your-oclc-secret"

# Optional: Batch processing control
export USE_BATCH_PROCESSING="auto"  # auto, true, or false
```

## Support

For questions or issues with batch processing:
1. Check the logs in the `logs/` folder
2. Review the detailed error messages
3. Try processing a small test batch first
4. Contact: Hannah Moutran - hlm2454@my.utexas.edu