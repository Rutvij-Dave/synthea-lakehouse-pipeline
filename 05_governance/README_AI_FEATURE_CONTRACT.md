# AI Feature Contract

Primary AI input:
`claims_lakehouse.gold.mv_ai_claim_features`

Grain:
1 row per `claim_id`.

The contract documents feature name, type, feature group, definition,
source object, grain, role and validation rule.

Use this contract as the stable interface between the Databricks data
platform team and the AI engineering team.

The AI team owns:
- feature selection/engineering beyond this contract
- anomaly models
- anomaly scores
- model registration
- explainability
- agent investigation packs
- investigator UI
