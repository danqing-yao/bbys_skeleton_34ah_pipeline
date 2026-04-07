# bbys_skeleton_34ah_pipeline
Barnebys Skeleton Pipeline - 34 Auction Houses

Measuring the incremental value Barnebys users bring to Skeleton auction houses at scale

🎯 Project Overview
This repository contains the full data pipeline for analyzing 34 Skeleton auction houses, measuring the value Barnebys users bring compared to other traffic sources across 2025.
An expansion of the [Olséns pilot analysis](link to pilot repo), this pipeline automates data extraction across multiple databases and processes them through a unified BigQuery analytics layer.
Key Metrics Analyzed:

User engagement (clicks, registrations, bids, winners)
Conversion rates (bidders → winners)
Revenue attribution (winning value, commissions, Skeleton fees)
Barnebys incremental value (price formation impact)
Auction house performance tiering (High / Mid / Low)

Time Period: January – December 2025 (excluding June–July)
Data Platform: Google BigQuery
Dataset: barnebys-skeleton.42ah
Auction Houses: 34 Skeleton clients

🏗️ Architecture Overview
Skeleton SQL Server (34 DBs)     AWS MySQL          Azure MySQL
         ↓                           ↓                   ↓
    extract.py ──────────────────────────────────────────┘
         ↓
    load.py → BigQuery (42ah dataset)
         ↓
    Raw Layer (SQL)
         ↓
    Processed Layer (SQL)
         ↓
    Analytics Layer (SQL)
         ↓
    Looker Studio Dashboard

🚀 Quick Start
Prerequisites

Python 3.x with dependencies: pyodbc, pymysql, pandas, sshtunnel, azure-identity, google-cloud-bigquery
Azure CLI (az login) for Skeleton SQL Server authentication
SSH key for AWS tunnel (bbys_tech_eu.pem)
BigQuery project access: barnebys-skeleton

Installation
bashpip install pyodbc pymysql pandas sshtunnel azure-identity google-cloud-bigquery
az login
Run the Pipeline
bash# Run full pipeline (extraction + SQL processing)
python main.py
```

`main.py` will:
1. Loop through all 34 auction houses → extract from Skeleton SQL Server
2. Extract lot data from AWS and Azure
3. Load all raw data to BigQuery
4. Run all SQL scripts in order

---

## 📁 Repository Structure
```
bbys_skeleton_34ah_pipeline/
├── README.md
├── config.py                          # Auction house list, DB connections, date ranges
├── extract.py                         # Extraction functions (Skeleton / AWS / Azure)
├── load.py                            # BigQuery loader
├── main.py                            # Pipeline orchestrator
└── sql/
    ├── update_raw_bite_bids.sql        # Fix currency for specific auction houses
    ├── raw_bbys_lots.sql               # Merge & deduplicate AWS + Azure lots
    ├── raw_bite_bids_clean.sql         # Deduplicate Bite bids
    ├── proc_skeleton_auctions_with_bite_bids.sql
    ├── proc_skeleton_auctions_with_enteredbids.sql
    ├── proc_skeleton_auctions_with_winning.sql
    ├── ana_lot_price_tiers.sql
    ├── ana_bids_with_price_tier.sql
    ├── ana_winning_bids_with_price_tier.sql
    ├── ana_barnebys_increment.sql
    ├── ana_funnel_all_stats.sql        # Wide table: all metrics by ah + source
    ├── ana_funnel_excluded67_unpivoted.sql  # Unpivoted long format
    ├── ana_funnel_excluded67.sql       # Final funnel with percentages & ah names
    └── ana_skeleton_fee.sql            # Skeleton fee calculation + bbys_tier

🗂️ Key Tables
Input Tables
TableDescriptionSourceraw_skeleton_auctionsAuction & inventory records from all 34 AHsSkeleton SQL Serverraw_skeleton_enteredbidsAll entered bid recordsSkeleton SQL Serverraw_bbys_aws_lotsLot metadata Nov 2024 – May 2025AWS MySQLraw_bbys_azure_lotsLot metadata Jun – Dec 2025Azure MySQLraw_bite_clicksClick events by auction house & monthBiteraw_bite_registrationsUser registration eventsBiteraw_skeleton_pricingFee structure per auction house (Fixed/Percent/Hybrid)Manualraw_currency_eurEUR exchange rates for 2025Manualraw_auction_houseAuction house reference dataManual
Output Tables
TableDescriptionproc_skeleton_auctions_with_enteredbidsBids matched to EnteredBid with WebUserid & sourceproc_skeleton_auctions_with_winningBids with winning flags & commissionsana_funnel_all_statsWide table: all metrics by auction house + user sourceana_funnel_excluded67_unpivotedLong format funnel metricsana_funnel_excluded67Final funnel with percentages, ordering & AH namesana_barnebys_incrementLot-level Barnebys price incrementana_skeleton_feeMonthly Skeleton fee per AH with bbys_tier classification

⚙️ Configuration
All pipeline parameters are defined in config.py:
pythonCONFIG = {
    "bq_project": "barnebys-skeleton",
    "bq_dataset": "42ah",
    "skeleton_start": "2025-01-01",
    "skeleton_end":   "2026-01-01",
    "bbys_aws_start": "2024-11-01",
    "bbys_aws_end":   "2025-05-31",
    "bbys_azure_start": "2025-06-01",
    "bbys_azure_end":   "2025-12-31",
}
To update date ranges or add auction houses, edit config.py only — no changes needed elsewhere.

⚠️ Known Issues & Limitations

Dataset naming: Folder and dataset named 42ah for historical reasons; actual pipeline covers 34 auction houses
Currency fixes: Two auction houses have hardcoded currency corrections in update_raw_bite_bids.sql (Bastionen → DKK, Dahlstroms → EUR)
June–July exclusion: Excluded from all analytics due to source tracking issues
Skeleton settlement timing: Commission settled by enddate, not bid timestamp — cross-month auctions may cause small period discrepancies
Azure AD token expiry: Long extraction loops may trigger token expiry; main.py handles automatic retry with az login
MyntAuktioner (2659): Special handling — only March, May, July months required in ana_skeleton_fee


📚 Documentation
Detailed Technical Documentation: [Link to Google Doc]
Related: [Olséns Pilot Analysis](link to pilot repo) — single auction house proof-of-concept this pipeline is based on