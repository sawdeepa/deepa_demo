# Data Quality Issues - BLS & Population Data Analysis

## Overview
This document outlines data quality issues discovered during analysis of BLS employment data and DataUSA population data for the Rearc Data Quest.

---

## 1. Invalid Period Values (Q05)

**Issue**: BLS data contains invalid quarterly period `Q05`  
**Expected Values**: Q01, Q02, Q03, Q04 (standard quarters)  
**Impact**: Invalid records skew quarterly analysis  
**Resolution**: Filtered out all Q05 records before analysis  

**Implementation**:
```python
bls_data = bls_data[bls_data['period'].str.strip().str.upper() != 'Q05'].copy()
```

---

## 2. Whitespace in String Fields

**Issue**: Leading/trailing whitespace in series_id and period columns  
**Impact**: String matching failures, duplicate values when comparing  
**Resolution**: Applied `.str.strip()` to all object columns  

---

## 3. Missing Column (footnote_codes)

**Issue**: `footnote_codes` column >80% null/empty values  
**Impact**: Low information value, clutters dataset  
**Resolution**: Dropped column during data loading  

---

## 4. Limited Population Data Coverage

**Issue**: Population data only available for years 2013-2022 (10 records)  
**Overlap**: Only 10 years of matching data  

**Impact on Analysis**:
- **Q1 (Population Stats)**: Limited to 2013-2018 range (6 records)
- **Q2 (Best Years)**: Population data unavailable for most BLS records
- **Q3 (Series Join)**: LEFT JOIN used to preserve BLS data even when population missing

**Design Decision for Q3**:
> **Used LEFT JOIN instead of INNER JOIN** to ensure all BLS series data remains visible in the report, even though population data exists for only 10 years. This prevents loss of historical employment data and allows analysis of trends beyond the population dataset's limited timeframe.

**Join Results**:
- Matched records: ~2.8% (population available)
- Unmatched records: ~97.2% (population NULL)
- All BLS employment data preserved for analysis

---

## 5. Temporal Completeness

**Issue**: Some series missing data for certain quarters/years  
**Findings**:
- ~45% of series have incomplete temporal coverage
- Many series missing Q1-Q3 data for recent years
- Expected 4 quarters per year, but some years have <4 quarters

**Impact**: Affects year-over-year trend analysis for incomplete series

---

## 6. Outliers in Value Column

**Issue**: Statistical outliers detected using IQR method (Q1 - 3×IQR, Q3 + 3×IQR)  
**Finding**: ~2-5% of records fall outside normal range  
**Assessment**: Outliers appear to be legitimate extreme values rather than errors (based on domain knowledge of employment metrics)  
**Action**: Retained outliers, flagged for awareness

---

## 7. Negative Values

**Issue**: ~1-2% of BLS value records contain negative numbers  
**Assessment**: Negative values are valid for certain employment metrics (e.g., productivity changes, rate changes)  
**Action**: Retained, documented as expected for certain series types

---

## Data Cleaning Applied

✅ Removed Q05 records  
✅ Trimmed whitespace from all string columns  
✅ Dropped footnote_codes column  
✅ Column name cleanup (stripped whitespace)  
✅ Used LEFT JOIN in Q3 to preserve all BLS data despite limited population coverage  


---

## Analysis Impact Summary

| Analysis | Records | Data Quality Impact |
|----------|---------|---------------------|
| Q1 - Population Stats | 6 years | Limited to 2013-2018 due to data availability |
| Q2 - Best Years | 87 series | Full BLS coverage, population unavailable for most |
| Q3 - Series + Population | 78 years | LEFT JOIN preserves all BLS data; 68 years have NULL population |

**Key Takeaway**: LEFT JOIN in Q3 ensures complete employment trend visibility despite sparse population data, allowing temporal analysis across the full BLS dataset history.
