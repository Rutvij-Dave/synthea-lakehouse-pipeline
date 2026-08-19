# Synthea Claims Lakehouse — Final Read Me

## 1. Project purpose

This repository implements the Databricks claims data platform for the use case:

**Claims Fraud & Anomaly Detection — "Detect, Explain, and Investigate with an Investigative Agent".**

The assigned problem requires a Delta-based claims ingestion and aggregation pipeline that produces claim-, patient-, and provider-level features, supports anomaly detection, and provides downstream investigation capabilities.

## 2. Architecture

```text
00_landing
     |
     v
01_bronze
     |
     v
02_silver
     |
     v
03_gold
     |
     +----------------------+
     |                      |
     v                      v
04_reporting          AI feature mart
                            |
                            v
                       AI Engineering
                            |
                +-----------+-----------+
                |           |           |
                v           v           v
             Models     Explainability  Agent
                |           |           |
                +-----------+-----------+
                            |
                            v
                    Investigator UI

05_governance
      |
      +---- DQ / dictionary / AI contract

tests
      |
      v
GitHub Actions
```

## 3. Repository structure

```text
synthea-lakehouse-pipeline/
|
├── 00_landing/
│   └── 01_fetch_data.py
|
├── 01_bronze/
│   ├── 02_fhir_r4.py
│   ├── 03_fhir_stu3.py
│   ├── 04_fhir_dstu2.py
│   ├── 05_ccda.py
│   └── 06_csv_core.py
|
├── 02_silver/
│   ├── 01_claim_fhir_r4.py
│   ├── 02_claim_dedup.py
│   ├── 03_patient_fhir_r4.py
│   ├── 04_provider_fhir_r4.py
│   ├── 05_organization_fhir_r4.py
│   ├── 06_encounter_fhir_r4.py
│   ├── 07_claim_line_fhir_r4.py
│   ├── 08_claim_adjudication_fhir_r4.py
│   └── 09_quarantine.py
|
├── 03_gold/
│   ├── 01_fact_claim.py
│   ├── 02_dim_patient.py
│   ├── 03_dim_provider.py
│   ├── 04_dim_organization.py
│   ├── 05_dim_payer.py
│   ├── 06_dim_encounter.py
│   ├── 07_dim_date.py
│   ├── 08_dim_claim_type.py
│   ├── 09_dim_source.py
│   ├── 10_fact_claim_line.py
│   ├── 11_fact_claim_adjudication.py
│   ├── 12_fact_encounter.py
│   └── 13_mv_ai_claim_features.sql
|
├── 04_reporting/
│   ├── 01_ai_feature_access.sql
│   ├── 01_report_claim_summary.py
│   ├── 02_report_provider_summary.py
│   ├── 03_report_claim_trends.py
│   └── 04_report_data_quality.py
|
├── 05_governance/
│   ├── 01_dq_governance.py
│   ├── 02_data_dictionary.py
│   └── 03_ai_feature_contract.py
|
├── tests/
│   ├── governance_tests.yml
│   ├── requirements.txt
│   ├── test_databricks_governance.py
│   ├── test_repository_structure.py
│   └── ai_feature_contract_validation.sql
|
├── queries/
│   └── ai_team_start_query.sql
|
└── .github/
    └── workflows/
        └── data-governance-tests.yml
```

## 4. Databricks catalogs and schemas

```text
claims_lakehouse.raw
claims_lakehouse.bronze
claims_lakehouse.silver
claims_lakehouse.gold
claims_lakehouse.reporting
claims_lakehouse.governance
```

The pipeline default catalog/schema can remain:

```text
Catalog: claims_lakehouse
Schema: bronze
```

All non-Bronze datasets use fully qualified three-part names.

## 5. Layer responsibilities

### Landing

Downloads and extracts source packages.

### Bronze

Preserves source-aligned data with ingestion metadata and source lineage.

Supported source families:
- FHIR R4
- FHIR STU3
- FHIR DSTU2
- C-CDA
- CSV

### Silver

Performs:
- canonicalization
- entity extraction
- deduplication
- claim-line normalization
- adjudication normalization
- quarantine
- data-quality handling

### Gold

Provides:
- dimensions
- explicit-grain facts
- claim-level analytical features
- AI-ready feature materialized view

### Reporting

Provides business/reporting datasets and the governed AI feature access view.

### Governance

Provides:
- DQ governance results
- data dictionary
- AI feature contract

### Tests

Provides:
- repository structure tests
- Databricks data-quality tests
- AI feature contract validation
- GitHub Actions automation

## 6. AI handoff

### Primary AI dataset

```text
claims_lakehouse.gold.mv_ai_claim_features
```

### Grain

```text
1 row per claim_id
```

### Current validated scale

```text
Claims:   60,970
Patients: 1,180
Providers: 1,035
```

### AI feature groups

- IDENTIFIER
- CLAIM
- FINANCIAL
- LINE
- TEMPORAL
- PATIENT
- PROVIDER
- ANOMALY
- DATA_QUALITY
- LINEAGE

### AI governance contract

```text
claims_lakehouse.governance.ai_feature_contract
```

The contract documents feature name, type, feature group, definition and validation rule.

Before modeling, AI engineers should inspect both:

```text
claims_lakehouse.gold.mv_ai_claim_features
claims_lakehouse.governance.ai_feature_contract
```

## 7. AI team start query

The single handoff query is:

```sql
SELECT
    claim_id,
    patient_id,
    provider_id,
    claim_type,
    claim_status,
    service_start_date,
    service_end_date,
    claim_created_ts,
    claim_total_amount,
    claim_total_currency,
    claim_line_count,
    line_count_from_detail,
    line_net_amount_total,
    avg_line_amount,
    max_line_amount,
    min_line_amount,
    claim_vs_line_amount_difference,
    claim_amount_per_line,
    claim_amount_log,
    service_duration_days,
    patient_claim_count,
    patient_total_claim_amount,
    patient_avg_claim_amount,
    patient_max_claim_amount,
    patient_avg_line_count,
    provider_claim_count,
    provider_unique_patient_count,
    provider_total_claim_amount,
    provider_avg_claim_amount,
    provider_max_claim_amount,
    provider_avg_line_count,
    claim_to_patient_avg_ratio,
    claim_to_provider_avg_ratio,
    high_amount_flag,
    multi_line_flag,
    missing_patient_flag,
    missing_provider_flag,
    _ingest_ts,
    _record_source
FROM claims_lakehouse.gold.mv_ai_claim_features
ORDER BY claim_created_ts, claim_id;
```

This is the **data-platform handoff**. AI engineering should not modify the Gold feature mart directly.

## 8. What the AI team still owns

The AI team should build:

1. feature selection / additional feature engineering
2. unsupervised anomaly detector
3. supervised detector when trusted labels are available
4. anomaly score and rank
5. model registration and versioning
6. explainability artifacts
7. investigation-ready case packs
8. natural-language case summaries
9. suggested drill-down queries
10. outreach / audit templates
11. investigator UI
12. exportable case bundles

The data platform provides the governed inputs for those components.

## 9. Important interpretation rule

`claim_vs_line_amount_difference` is an analytical/reconciliation feature.

It must not automatically be interpreted as proof of fraud or as a hard data-quality failure because the underlying claim and line-level monetary fields can represent different financial concepts.

The AI team should evaluate this feature in combination with other signals.

## 10. Data quality checks already demonstrated

The current AI feature mart has been validated for:

```text
60,970 total feature rows
60,970 unique claims
0 missing patient IDs
0 missing provider IDs
0 missing claim amounts
0 missing claim line counts
0 negative claim amounts
1,180 unique patients
1,035 unique providers
```

The feature contract has also been checked in both directions:

```text
Contract -> Feature Mart: PASS
Feature Mart -> Contract: PASS
```

## 11. GitHub Actions

Workflow:

```text
.github/workflows/data-governance-tests.yml
```

Repository secrets required for Databricks integration tests:

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
DATABRICKS_WAREHOUSE_ID
```

Repository variable:

```text
RUN_DATABRICKS_TESTS=true
```

## 12. Development rule

Do not allow AI/model code to bypass the governed feature mart.

Preferred dependency:

```text
AI models
   |
   v
claims_lakehouse.gold.mv_ai_claim_features
   |
   v
governed feature contract
```

Do not build anomaly models directly against Bronze raw payloads.

## 13. Final ownership boundary

### Data Platform / Databricks

Owns:
- ingestion
- medallion layers
- deduplication
- DQ
- quarantine
- dimensions
- facts
- reporting
- AI feature mart
- AI feature contract
- CI/CD quality checks

### AI Engineering

Owns:
- models
- anomaly scores
- model registry
- explainability
- investigation agent
- case generation
- investigator UI
- exportable investigation bundles

This separation gives the AI team a stable, governed interface while allowing the model/agent implementation to evolve independently.
