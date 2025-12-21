"""
Lambda function for Rearc Data Quest Part 3 Analytics
Performs data cleanup and analysis on BLS employment and DataUSA population data
"""

import json
import logging
import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 configuration from environment variables
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'rearc-deepa-demo')
BLS_DATA_KEY = os.environ.get('BLS_DATA_KEY', 'raw/pr/pr.data.0.Current')
POPULATION_PREFIX = os.environ.get('POPULATION_PREFIX', 'raw/datausa/population/')


def load_bls_data(s3_client):
    """Load and clean BLS employment data from S3"""
    logger.info(f"Loading BLS data from s3://{BUCKET_NAME}/{BLS_DATA_KEY}")
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=BLS_DATA_KEY)
    bls_data = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')), sep='\t')
    
    # Clean column names
    bls_data.columns = bls_data.columns.str.strip()
    
    # Drop footnote_codes if exists
    if 'footnote_codes' in bls_data.columns:
        bls_data = bls_data.drop(columns=['footnote_codes'])
    
    # Trim whitespace from all string columns
    for col in bls_data.select_dtypes(include='object').columns:
        bls_data[col] = bls_data[col].str.strip()
    
    logger.info(f"Loaded and cleaned BLS data: {len(bls_data):,} rows, {len(bls_data.columns)} columns")
    return bls_data


def load_population_data(s3_client):
    """Load DataUSA population data from S3"""
    logger.info(f"Loading population data from s3://{BUCKET_NAME}/{POPULATION_PREFIX}")
    
    # List all population files and get the latest
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=POPULATION_PREFIX)
    
    if 'Contents' not in response:
        logger.warning("No population data found")
        return pd.DataFrame()
    
    # Get the most recent file
    latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
    pop_key = latest_file['Key']
    
    logger.info(f"Loading latest population file: {pop_key}")
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=pop_key)
    pop_json = json.loads(obj['Body'].read().decode('utf-8'))
    
    # Handle different JSON structures
    if 'data' in pop_json:
        pop_data = pd.DataFrame(pop_json['data'])
    elif isinstance(pop_json, list):
        pop_data = pd.DataFrame(pop_json)
    else:
        pop_data = pd.DataFrame([pop_json])
    
    logger.info(f"Loaded population data: {len(pop_data):,} rows, {len(pop_data.columns)} columns")
    return pop_data


def analyze_q1_population_stats(pop_data):
    """Q1: Calculate population statistics for years 2013-2018"""
    logger.info("Starting Q1 Analysis: Population Statistics (2013-2018)")
    
    if len(pop_data) == 0:
        logger.warning("Q1: No population data available")
        return None
    
    # Filter for years 2013-2018
    pop_filtered = pop_data[(pop_data['Year'] >= 2013) & (pop_data['Year'] <= 2018)].copy()
    
    if len(pop_filtered) == 0:
        logger.warning("Q1: No data for years 2013-2018")
        return "No Data"
    
    mean_pop = pop_filtered['Population'].mean()
    std_pop = pop_filtered['Population'].std()
    
    result = {
        'analysis': 'Q1 - Population Statistics (2013-2018)',
        'mean_population': float(mean_pop),
        'std_dev_population': float(std_pop),
        'record_count': len(pop_filtered),
        'years': '2013-2018'
    }
    
    logger.info(f"Q1 Result: Mean={mean_pop:.0f}, StdDev={std_pop:.0f}, Records={len(pop_filtered)}")
    return result


def analyze_q2_best_years(bls_data):
    """Q2: Find best year per BLS series (year with max sum of quarterly values)"""
    logger.info("Starting Q2 Analysis: Best Year per BLS Series")
    
    if len(bls_data) == 0:
        logger.warning("Q2: No BLS data available")
        return "No BLS data available"
    
    # Calculate yearly sums for each series
    yearly_sums = bls_data.groupby(['series_id', 'year'])['value'].sum().reset_index()
    
    # Find best year per series (year with max value)
    best_years = yearly_sums.loc[yearly_sums.groupby('series_id')['value'].idxmax()].sort_values('series_id').reset_index(drop=True)
    
    # Convert to dictionary format
    best_years_dict = best_years.set_index('series_id')[['year', 'value']].to_dict('index')
    
    result = {
        'analysis': 'Q2 - Best Year per BLS Series',
        'total_series': len(best_years),
        'best_years': best_years_dict
    }
    
    logger.info(f"Q2 Result: Analyzed {len(best_years)} series : {best_years_dict}")
    return result


def analyze_q3_series_with_population(bls_data, pop_data):
    """Q3: Generate report for series PRS30006032 period Q01 with population data"""
    logger.info("Starting Q3 Analysis: Series PRS30006032 Q01 + Population")
    
    target_series = 'PRS30006032'
    target_period = 'Q01'
    
    # Filter BLS data for target series and period
    series_q01 = bls_data[
        (bls_data['series_id'].str.contains(target_series)) & 
        (bls_data['period'] == target_period)
    ].copy()
    
    if len(series_q01) == 0:
        logger.warning(f"Q3: No data found for series {target_series} period {target_period}")
        return None
    
    if len(pop_data) == 0:
        logger.warning(f"Q3: No population data available, returning {len(series_q01)} BLS records only")
        result = {
            'analysis': f'Q3 - Series {target_series} {target_period} (BLS data only)',
            'record_count': len(series_q01),
            'data': series_q01.to_dict('records')
        }
        return result
    
    # Merge with population data
    series_q01['year'] = series_q01['year'].astype(int)
    pop_data['Year'] = pop_data['Year'].astype(int)
    
    final_report = series_q01.merge(
        pop_data[['Year', 'Population']], 
        left_on='year', 
        right_on='Year', 
        how='left'
    )[['series_id', 'year', 'period', 'value', 'Population']].sort_values('year')
    
    if len(final_report) == 0:
        logger.warning("Q3: No matching data after merge with population")
        return {"error": "No matching data after merge with population"}
    
    year_range = f"{final_report['year'].min()}-{final_report['year'].max()}"
    result = {
        'analysis': f'Q3 - Series {target_series} {target_period} + Population',
        'series_id': target_series,
        'period': target_period,
        'record_count': len(final_report),
        'year_range': year_range,
        'data': final_report.to_dict('records')
    }
    
    logger.info(f"Q3 Result: {final_report.to_dict('records')}")
    return result


def lambda_handler(event, context):
    """
    Lambda handler for Part 3 analytics
    
    Args:
        event: Lambda event object
        context: Lambda context object
        
    Returns:
        dict: Response with status code and analysis results
    """
    try:
        logger.info("Starting Rearc Data Quest Part 3 Analytics Lambda")
        logger.info(f"Event: {json.dumps(event)}")
        
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name='eu-north-1')
        
        # Load and clean data
        bls_data = load_bls_data(s3_client)
        pop_data = load_population_data(s3_client)
        
        # Perform all analyses
        q1 = analyze_q1_population_stats(pop_data)
        q2 = analyze_q2_best_years(bls_data)
        q3 = analyze_q3_series_with_population(bls_data, pop_data)
        
        # Compile results
        results = {
            'timestamp': datetime.now().isoformat(),
            'status': 'success',
            'analyses': {
                'Q1_population_stats': q1,
                'Q2_best_years': q2,
                'Q3_series_with_population': q3
            }
        }
        
        logger.info("All analyses completed successfully, Results are logged")
        
        # Save results to S3
        try:
            results_key = f"analytics/results/analysis_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=results_key,
                Body=json.dumps(results, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Results saved to S3: s3://{BUCKET_NAME}/{results_key}")
            results['s3_location'] = f"s3://{BUCKET_NAME}/{results_key}"
        except Exception as e:
            logger.error(f"Failed to save results to S3: {str(e)}")
            results['s3_save_error'] = str(e)

        return {
            'statusCode': 200,
            'body': json.dumps(results, indent=2),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }

