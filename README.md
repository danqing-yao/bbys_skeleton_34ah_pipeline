# Barnebys Skeleton Pipeline - 34 Auction Houses

Measuring the incremental value Barnebys users bring to Skeleton auction houses at scale

---

## 🎯 Project Overview

This repository contains the full data pipeline for analyzing **34 Skeleton auction houses**, measuring the value Barnebys users bring compared to other traffic sources across 2025.

An expansion of the [Olséns pilot analysis](link: https://github.com/danqing-yao/pilot-olsens), this pipeline automates data extraction across multiple databases and processes them through a unified BigQuery analytics layer.

### Key Metrics Analyzed

- User engagement (Clicks, registrations, bids, winners)
- Bidding behavior (Price formation impact, Bbys incremental value, category segment)
- Revenue attribution (Value/influence, efficiency, monetisation)
- Auction house performance tiering (High / Mid / Low)

### Project Scope

- **Time Period:** January – December 2025 (excluding June–July)  
- **Data Platform:** Google BigQuery  
- **Dataset:** `barnebys-skeleton.42ah`  
- **Auction Houses:** 34 Skeleton clients  

---

## 🏗️ Architecture Overview

```text
                                              Manual CSV Upload
                                         (auction_house, currency_eur,
                                          skeleton_pricing)
                                                     ↓
Skeleton SQL Server (34 DBs)    AWS MySQL    Azure MySQL    BigQuery (Bite tables)
         ↓                          ↓             ↓                    ↓
         └──────────── extract.py ────────────────┘                    │
                           ↓                                           │
                        load.py                                        │
                           ↓                                           │
                    BigQuery .42ah dataset ────────────────────────────┘
                           ↓
                     Raw Layer (SQL)
                     (incl. direct queries to Bite tables)
                           ↓
                   Processed Layer (SQL)
                           ↓
                   Analytics Layer (SQL)
                           ↓
                  Looker Studio Dashboard
```



---

## 🚀 Quick Start

### Prerequisites

- Python 3.x  
- Dependencies:
  - `pyodbc`
  - `pymysql`
  - `pandas`
  - `sshtunnel`
  - `azure-identity`
  - `google-cloud-bigquery`
- Azure CLI (`az login`)
- SSH key for AWS tunnel (`bbys_tech_eu.pem`)
- BigQuery project access: `barnebys-skeleton`

---

### Installation

```bash
pip install pyodbc pymysql pandas sshtunnel azure-identity google-cloud-bigquery
az login
```

**Run the Pipeline**

```bash
# Run full pipeline (extraction + SQL processing)
python main.py
```

main.py will:

- Loop through all 34 auction houses → extract from Skeleton SQL Server
- Extract lot data from AWS and Azure
- Load all raw data to BigQuery
- Run all SQL scripts in order

## 📁 Repository Structure

```
bbys_skeleton_34ah_pipeline/
├── README.md
├── config.py                          # Auction house list, DB connections, date ranges
├── extract.py                         # Extraction functions (Skeleton / AWS / Azure)
├── load.py                            # BigQuery loader
├── main.py                            # Pipeline orchestrator
└── sql/
    ├── update_raw_bite_bids.sql
    ├── raw_bbys_lots.sql
    ├── raw_bite_bids_clean.sql
    ├── proc_skeleton_auctions_with_bite_bids.sql
    ├── proc_skeleton_auctions_with_enteredbids.sql
    ├── proc_skeleton_auctions_with_winning.sql
    ├── ana_lot_price_tiers.sql
    ├── ana_bids_with_price_tier.sql
    ├── ana_winning_bids_with_price_tier.sql
    ├── ana_barnebys_increment.sql
    ├── ana_funnel_all_stats.sql
    ├── ana_funnel_excluded67_unpivoted.sql
    ├── ana_funnel_excluded67.sql
    └── ana_skeleton_fee.sql
└── Tables/Google Drive
```

---

## 🗂️ Key Tables

### Input Tables

| Table                    | Description                        | Source              |
| ------------------------ | ---------------------------------- | ------------------- |
| raw_skeleton_auctions    | Auction & inventory records        | Skeleton SQL Server |
| raw_skeleton_enteredbids | All entered bid records            | Skeleton SQL Server |
| raw_bbys_aws_lots        | Lot metadata (Nov 2024 – May 2025) | AWS MySQL           |
| raw_bbys_azure_lots      | Lot metadata (Jun – Dec 2025)      | Azure MySQL         |
| raw_bite_clicks          | Click events                       | Bite BigQuery       |
| raw_bite_registrations   | Registration events                | Bite BigQuery       |
| raw_bite_bids            | Bid events                         | Bite BigQuery       |
| raw_skeleton_pricing     | Fee structure per auction house    | Manual              |
| raw_currency_eur         | EUR exchange rates (2025)          | Manual              |
| raw_auction_house        | Auction house reference data       | Manual              |

### Output Tables

| Table                                   | Description                               |
| --------------------------------------- | ----------------------------------------- |
| proc_skeleton_auctions_with_enteredbids | Bids matched with EnteredBid              |
| proc_skeleton_auctions_with_winning     | Winning bids & commissions                |
| ana_lot_price_tiers                     | Lot-level price tier classification       |
| ana_bids_with_price_tier                | All bids enriched with price tier         |
| ana_winning_bids_with_price_tier        | All winning bids enriched with price tier |
| ana_funnel_all_stats                    | Wide metrics table                        |
| ana_funnel_excluded67_unpivoted         | Long format funnel                        |
| ana_funnel_excluded67                   | Final funnel                              |
| ana_barnebys_increment                  | Lot-level price increment                 |
| ana_skeleton_fee                        | Monthly fee per AH                        |


---

## ⚙️ Configuration

All pipeline parameters are defined in **config.py**:

```Python
CONFIG = {
    "bq_project": "barnebys-skeleton",
    "bq_dataset": "42ah",
    "skeleton_start": "2025-01-01",
    "skeleton_end":   "2026-01-01",
    "bbys_aws_start": "2024-11-01",
    "bbys_aws_end":   "2025-05-31",
    "bbys_azure_start": "2025-06-01",
    "bbys_azure_end":   "2025-12-31",
}
```

👉 To update date ranges or auction houses, edit only **config.py**

---

## ⚠️ Known Issues & Limitations

- Dataset naming: 42ah is historical; actual pipeline is 34 auction houses

- Currency fixes: Hardcoded in update_raw_bite_bids.sql
  Bastionen → DKK
  Dahlstroms → EUR

- June–July exclusion: Missing tracking data

- Skeleton settlement timing: Based on enddate, not bid timestamp

- Azure AD token expiry: Auto-retry handled in main.py

- MyntAuktioner (2659): Special month handling in ana_skeleton_fee

---
## 📚 Dashboard

Access: https://lookerstudio.google.com/s/ih_glLZb8FA

**How to Refresh**
- Re-run python main.py
- Open Looker Studio dashboard
- Click Refresh data on the data source connected to barnebys-skeleton.42ah

---

## 📚 Documentation

**Full Technical Documentation (Google Docs):**  
🔗 https://docs.google.com/document/d/1UemCfsODSEHuGlu_8wZPwi1mgvJoPzobT7vLrE_svI0/edit?usp=sharing

Covers: 
- Executive Summary
- Business Context
- Data Architecture
- Python Extraction Layer
- Data Processing Logic
- Metrics Definition
- Known Issues & Limitations
- SQL Scripts Reference
- Long-term Strategy and Scalability
- Dashboard
- Appendix

---