from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, Response
from datetime import datetime, timedelta
import random, uuid, json, csv, io, os

app = Flask(__name__)
app.secret_key = "docval-2026"

# ─────────────────────────────────────────────────────────────────
# ENUMERATIONS
# ─────────────────────────────────────────────────────────────────
MARKET_SEGMENTS = ["Large Group", "Small Group"]
GROUP_TYPES      = ["FB", "FHCP"]
RATE_TYPES       = ["Composite", "BalancedFunding", "MLB"]
CONTRACT_TYPES   = ["ACA", "KYP"]
CENSUS_TIERS     = [2, 3, 4, 7]
APPLICATIONS     = ["ContentCentral", "SellPoint", "EnterCalc"]
DOC_NAMES        = [
    "AgentAcknowledgment", "SG Renewal Letter", "Group Information Report",
    "ACA Renewal Census", "Enrollment Summary", "Composite Rates",
    "Coverage Summary", "Premium Notice", "Benefit Summary",
]
DOC_TYPES        = ["PDF", "XML", "DOCX", "JSON"]
PAY_TYPES        = ["Document", "Response", "Request"]

# ─────────────────────────────────────────────────────────────────
# MOCK DATA  (30 transactions)
# ─────────────────────────────────────────────────────────────────
random.seed(42)

def _tx(id_, uuid_, case, grp, grp_name, doc, sub, dtype, payload, app_, validated,
        market_seg, grp_type, rate_type, contract, tiers, req_time):
    return dict(id=id_, uuid=uuid_, case_id=case, group_number=grp, group_name=grp_name,
                doc_name=doc, sub_doc_name=sub, doc_type=dtype, payload_type=payload,
                application=app_, validated=validated, market_segment=market_seg,
                group_type=grp_type, rate_type=rate_type, contract_type=contract,
                census_tiers=tiers, request_time=req_time)

BASE_UUID = "8b927cef-af01-4736-b2df-ffd9fd9a33de"

TRANSACTIONS = [
    _tx(1033499, f"{BASE_UUID}-DOC-1",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "PDF",  "Document", "ContentCentral", False, "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:19"),
    _tx(1033495, f"{BASE_UUID}-DOC-2",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "XML",  "Response", "SellPoint",      False, "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:19"),
    _tx(1033487, f"{BASE_UUID}-DOC-3",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "PDF",  "Document", "EnterCalc",      True,  "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:18"),
    _tx(1033486, f"{BASE_UUID}-DOC-4",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "DOCX", "Document", "EnterCalc",      True,  "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:18"),
    _tx(1033485, f"{BASE_UUID}-DOC-5",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "XML",  "Response", "EnterCalc",      False, "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:18"),
    _tx(1033484, f"{BASE_UUID}-DOC-6",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "XML",  "Request",  "EnterCalc",      False, "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:16"),
    _tx(1033474, f"{BASE_UUID}-DOC-7",  "LG-F-59267", "EB23+",  "EastBlue Corp",         "AgentAcknowledgment",    "AgentAcknowledgment",    "XML",  "Request",  "SellPoint",      False, "Large Group", "FB",   "Composite",       "ACA", 2, "2026-03-29 09:21:16"),
    _tx(1033420, "c731c51d-1858-43eb-a335-001",        "SG-R-20110", "K9196", "BlueStar LLC",   "SG Renewal Letter",      "SG Renewal Letter",      "PDF",  "Document", "ContentCentral", True,  "Small Group", "FHCP", "BalancedFunding", "ACA", 3, "2026-03-27 11:00:00"),
    _tx(1033421, "c731c51d-1858-43eb-a335-002",        "SG-R-20110", "K9196", "BlueStar LLC",   "SG Renewal Letter",      "ACA Renewal Census",     "XML",  "Response", "SellPoint",      True,  "Small Group", "FHCP", "BalancedFunding", "ACA", 3, "2026-03-27 11:01:00"),
    _tx(1033422, "c731c51d-1858-43eb-a335-003",        "SG-R-20110", "K9196", "BlueStar LLC",   "SG Renewal Letter",      "Enrollment Summary",     "PDF",  "Document", "EnterCalc",      False, "Small Group", "FHCP", "BalancedFunding", "ACA", 3, "2026-03-27 11:02:00"),
    _tx(1033380, "1e88a4d3-b203-4079-8984-001",        "LG-I-79338", "GR882","Horizon Health",  "Group Information Report","Group Information Report","PDF", "Document", "ContentCentral", True,  "Large Group", "FB",   "MLB",             "KYP", 4, "2026-03-27 12:00:00"),
    _tx(1033381, "1e88a4d3-b203-4079-8984-002",        "LG-I-79338", "GR882","Horizon Health",  "Group Information Report","Group Information Report","XML", "Response", "EnterCalc",      True,  "Large Group", "FB",   "MLB",             "KYP", 4, "2026-03-27 12:01:00"),
    _tx(1033382, "1e88a4d3-b203-4079-8984-003",        "LG-I-79338", "GR882","Horizon Health",  "Coverage Summary",       "Coverage Summary",       "PDF",  "Document", "SellPoint",      False, "Large Group", "FB",   "MLB",             "KYP", 4, "2026-03-27 12:02:00"),
    _tx(1033300, "ae77b201-cc44-4a91-bb01-001",        "SG-X-10041", "M3312","MediGroup Plus",  "Composite Rates",        "Composite Rates",        "XML",  "Request",  "ContentCentral", False, "Small Group", "FB",   "Composite",       "KYP", 7, "2026-03-26 08:30:00"),
    _tx(1033301, "ae77b201-cc44-4a91-bb01-002",        "SG-X-10041", "M3312","MediGroup Plus",  "Composite Rates",        "Composite Rates",        "PDF",  "Document", "EnterCalc",      False, "Small Group", "FB",   "Composite",       "KYP", 7, "2026-03-26 08:31:00"),
    _tx(1033302, "ae77b201-cc44-4a91-bb01-003",        "SG-X-10041", "M3312","MediGroup Plus",  "Premium Notice",         "Premium Notice",         "DOCX", "Document", "SellPoint",      True,  "Small Group", "FB",   "Composite",       "KYP", 7, "2026-03-26 08:32:00"),
    _tx(1033250, "bf33a109-ee55-5bc2-cc02-001",        "LG-P-88210", "P7740","PrimeCare Inc",   "Benefit Summary",        "Benefit Summary",        "PDF",  "Document", "ContentCentral", True,  "Large Group", "FHCP", "BalancedFunding", "ACA", 2, "2026-03-25 14:15:00"),
    _tx(1033251, "bf33a109-ee55-5bc2-cc02-002",        "LG-P-88210", "P7740","PrimeCare Inc",   "Benefit Summary",        "Benefit Summary",        "XML",  "Response", "EnterCalc",      True,  "Large Group", "FHCP", "BalancedFunding", "ACA", 2, "2026-03-25 14:16:00"),
    _tx(1033252, "bf33a109-ee55-5bc2-cc02-003",        "LG-P-88210", "P7740","PrimeCare Inc",   "Enrollment Summary",     "Enrollment Summary",     "PDF",  "Document", "SellPoint",      False, "Large Group", "FHCP", "BalancedFunding", "ACA", 2, "2026-03-25 14:17:00"),
    _tx(1033200, "da44b112-ff66-6cd3-dd03-001",        "SG-Q-33119", "Q5501","QuickCare Group", "AgentAcknowledgment",    "AgentAcknowledgment",    "PDF",  "Document", "ContentCentral", False, "Small Group", "FHCP", "MLB",             "ACA", 3, "2026-03-24 09:00:00"),
    _tx(1033201, "da44b112-ff66-6cd3-dd03-002",        "SG-Q-33119", "Q5501","QuickCare Group", "ACA Renewal Census",     "ACA Renewal Census",     "XML",  "Request",  "SellPoint",      False, "Small Group", "FHCP", "MLB",             "ACA", 3, "2026-03-24 09:01:00"),
    _tx(1033202, "da44b112-ff66-6cd3-dd03-003",        "SG-Q-33119", "Q5501","QuickCare Group", "Coverage Summary",       "Coverage Summary",       "PDF",  "Document", "EnterCalc",      True,  "Small Group", "FHCP", "MLB",             "ACA", 3, "2026-03-24 09:02:00"),
    _tx(1033150, "ea55c123-aa77-7de4-ee04-001",        "LG-T-55004", "T1190","TruHealth Corp",  "SG Renewal Letter",      "SG Renewal Letter",      "PDF",  "Document", "ContentCentral", True,  "Large Group", "FB",   "Composite",       "KYP", 4, "2026-03-23 10:30:00"),
    _tx(1033151, "ea55c123-aa77-7de4-ee04-002",        "LG-T-55004", "T1190","TruHealth Corp",  "Group Information Report","Group Information Report","DOCX","Document", "EnterCalc",      True,  "Large Group", "FB",   "Composite",       "KYP", 4, "2026-03-23 10:31:00"),
    _tx(1033152, "ea55c123-aa77-7de4-ee04-003",        "LG-T-55004", "T1190","TruHealth Corp",  "Premium Notice",         "Premium Notice",         "XML",  "Response", "SellPoint",      False, "Large Group", "FB",   "Composite",       "KYP", 4, "2026-03-23 10:32:00"),
    _tx(1033100, "fb66d134-bb88-8ef5-ff05-001",        "SG-V-71230", "V3320","VitalCare HMO",   "Composite Rates",        "Composite Rates",        "PDF",  "Document", "ContentCentral", False, "Small Group", "FB",   "BalancedFunding", "ACA", 7, "2026-03-22 13:00:00"),
    _tx(1033101, "fb66d134-bb88-8ef5-ff05-002",        "SG-V-71230", "V3320","VitalCare HMO",   "Enrollment Summary",     "Enrollment Summary",     "XML",  "Request",  "SellPoint",      False, "Small Group", "FB",   "BalancedFunding", "ACA", 7, "2026-03-22 13:01:00"),
    _tx(1033050, "gc77e145-cc99-9fg6-gg06-001",        "LG-W-89001", "W9981","WellPath Benefit", "Benefit Summary",       "Benefit Summary",        "PDF",  "Document", "ContentCentral", True,  "Large Group", "FHCP", "MLB",             "KYP", 2, "2026-03-21 08:00:00"),
    _tx(1033051, "gc77e145-cc99-9fg6-gg06-002",        "LG-W-89001", "W9981","WellPath Benefit", "Coverage Summary",      "Coverage Summary",       "XML",  "Response", "EnterCalc",      True,  "Large Group", "FHCP", "MLB",             "KYP", 2, "2026-03-21 08:01:00"),
    _tx(1033052, "gc77e145-cc99-9fg6-gg06-003",        "LG-W-89001", "W9981","WellPath Benefit", "AgentAcknowledgment",   "AgentAcknowledgment",    "DOCX", "Document", "SellPoint",      False, "Large Group", "FHCP", "MLB",             "KYP", 2, "2026-03-21 08:02:00"),
]

# ─────────────────────────────────────────────────────────────────
# VALIDATIONS  (rich mock data)
# ─────────────────────────────────────────────────────────────────
VALIDATIONS = [
    {"incident_id": "8b927cef-a_20260329_100507",
     "transaction_id": f"{BASE_UUID}-DOC-3",
     "doc_name": "AgentAcknowledgment", "sub_doc_name": "AgentAcknowledgment",
     "application": "EnterCalc", "case_id": "LG-F-59267",
     "group_number": "EB23+", "group_name": "EastBlue Corp",
     "market_segment": "Large Group", "group_type": "FB",
     "rate_type": "Composite", "contract_type": "ACA", "census_tiers": 2,
     "status": "Success",  "match_rate": 100.0, "mismatches": 0, "total_elements": 15,
     "created_date": "2026-03-29 10:05"},
    {"incident_id": "c731c51d-1_20260327_123434_sdv20",
     "transaction_id": "c731c51d-1858-43eb-a335-001",
     "doc_name": "SG Renewal Letter", "sub_doc_name": "SG Renewal Letter",
     "application": "SellPoint", "case_id": "SG-R-20110",
     "group_number": "K9196", "group_name": "BlueStar LLC",
     "market_segment": "Small Group", "group_type": "FHCP",
     "rate_type": "BalancedFunding", "contract_type": "ACA", "census_tiers": 3,
     "status": "Success",  "match_rate": 100.0, "mismatches": 0, "total_elements": 14,
     "created_date": "2026-03-27 12:35"},
    {"incident_id": "c731c51d-1_20260327_123434_sdv24",
     "transaction_id": "c731c51d-1858-43eb-a335-002",
     "doc_name": "ACA Renewal Census", "sub_doc_name": "ACA Renewal Census",
     "application": "ContentCentral", "case_id": "SG-R-20110",
     "group_number": "K9196", "group_name": "BlueStar LLC",
     "market_segment": "Small Group", "group_type": "FHCP",
     "rate_type": "BalancedFunding", "contract_type": "ACA", "census_tiers": 3,
     "status": "Success",  "match_rate": 95.5, "mismatches": 2, "total_elements": 22,
     "created_date": "2026-03-27 12:35"},
    {"incident_id": "c731c51d-1_20260327_123434_sdv19",
     "transaction_id": "c731c51d-1858-43eb-a335-003",
     "doc_name": "Enrollment Summary", "sub_doc_name": "Enrollment Summary",
     "application": "EnterCalc", "case_id": "SG-R-20110",
     "group_number": "K9196", "group_name": "BlueStar LLC",
     "market_segment": "Small Group", "group_type": "FHCP",
     "rate_type": "BalancedFunding", "contract_type": "ACA", "census_tiers": 3,
     "status": "Mismatch", "match_rate": 88.9, "mismatches": 3, "total_elements": 27,
     "created_date": "2026-03-27 12:34"},
    {"incident_id": "1e88a4d3-b_20260327_122551",
     "transaction_id": "1e88a4d3-b203-4079-8984-001",
     "doc_name": "Group Information Report", "sub_doc_name": "Group Information Report",
     "application": "SellPoint", "case_id": "LG-I-79338",
     "group_number": "GR882", "group_name": "Horizon Health",
     "market_segment": "Large Group", "group_type": "FB",
     "rate_type": "MLB", "contract_type": "KYP", "census_tiers": 4,
     "status": "Success",  "match_rate": 100.0, "mismatches": 0, "total_elements": 36,
     "created_date": "2026-03-27 12:26"},
    {"incident_id": "ae77b201-r_20260326_083500",
     "transaction_id": "ae77b201-cc44-4a91-bb01-002",
     "doc_name": "Composite Rates", "sub_doc_name": "Composite Rates",
     "application": "ContentCentral", "case_id": "SG-X-10041",
     "group_number": "M3312", "group_name": "MediGroup Plus",
     "market_segment": "Small Group", "group_type": "FB",
     "rate_type": "Composite", "contract_type": "KYP", "census_tiers": 7,
     "status": "Mismatch", "match_rate": 82.4, "mismatches": 5, "total_elements": 28,
     "created_date": "2026-03-26 09:15"},
    {"incident_id": "bf33a109-r_20260325_141600",
     "transaction_id": "bf33a109-ee55-5bc2-cc02-001",
     "doc_name": "Benefit Summary", "sub_doc_name": "Benefit Summary",
     "application": "EnterCalc", "case_id": "LG-P-88210",
     "group_number": "P7740", "group_name": "PrimeCare Inc",
     "market_segment": "Large Group", "group_type": "FHCP",
     "rate_type": "BalancedFunding", "contract_type": "ACA", "census_tiers": 2,
     "status": "Success",  "match_rate": 97.8, "mismatches": 1, "total_elements": 45,
     "created_date": "2026-03-25 15:30"},
    {"incident_id": "ea55c123-r_20260323_103100",
     "transaction_id": "ea55c123-aa77-7de4-ee04-001",
     "doc_name": "SG Renewal Letter", "sub_doc_name": "SG Renewal Letter",
     "application": "SellPoint", "case_id": "LG-T-55004",
     "group_number": "T1190", "group_name": "TruHealth Corp",
     "market_segment": "Large Group", "group_type": "FB",
     "rate_type": "Composite", "contract_type": "KYP", "census_tiers": 4,
     "status": "Mismatch", "match_rate": 91.3, "mismatches": 4, "total_elements": 46,
     "created_date": "2026-03-23 11:45"},
    {"incident_id": "gc77e145-r_20260321_080100",
     "transaction_id": "gc77e145-cc99-9fg6-gg06-001",
     "doc_name": "Benefit Summary", "sub_doc_name": "Benefit Summary",
     "application": "ContentCentral", "case_id": "LG-W-89001",
     "group_number": "W9981", "group_name": "WellPath Benefit",
     "market_segment": "Large Group", "group_type": "FHCP",
     "rate_type": "MLB", "contract_type": "KYP", "census_tiers": 2,
     "status": "Success",  "match_rate": 100.0, "mismatches": 0, "total_elements": 33,
     "created_date": "2026-03-21 09:00"},
]

# ─────────────────────────────────────────────────────────────────
# EXTRACTION ROWS  (detail view)
# ─────────────────────────────────────────────────────────────────
EXTRACTION_ROWS = [
    {"category": "group_details", "data_element": "Group_Name",
     "doc_value": "LG_NEW_SALE_20260329083924", "sumapp_value": "LG_NEW_SALE_20260329083924",
     "entercalc_value": "LG_NEW_SALE_20260329083924", "plan": "N/A",
     "comparison": "Match"},
    {"category": "group_details", "data_element": "Group_Number",
     "doc_value": "EB23+", "sumapp_value": "EB23+", "entercalc_value": "EB23+",
     "plan": "N/A", "comparison": "Match"},
    {"category": "group_details", "data_element": "Effective_Date",
     "doc_value": "06/01/2026", "sumapp_value": "2026-06-01", "entercalc_value": "06/01/2026",
     "plan": "N/A", "comparison": "Mismatch"},
    {"category": "group_details", "data_element": "Contract_Period",
     "doc_value": "06/01/2026 – 05/31/2027", "sumapp_value": "06/01/2026 – 05/31/2027",
     "entercalc_value": "06/01/2026 – 05/31/2027", "plan": "N/A", "comparison": "Match"},
    {"category": "agent_details", "data_element": "PrimaryAgentName",
     "doc_value": "AgentTester001 Automation", "sumapp_value": "AgentTester001 Automation",
     "entercalc_value": "AgentTester001 Automation", "plan": "N/A", "comparison": "Match"},
    {"category": "agent_details", "data_element": "PrimaryAgencyName",
     "doc_value": "SELLPOINT IVP TEST BS AGENCY - 303Z",
     "sumapp_value": "SELLPOINT IVP TEST BS AGENCY - 303Z",
     "entercalc_value": "SELLPOINT IVP TEST BS AGENCY - 303Z", "plan": "N/A", "comparison": "Match"},
    {"category": "agent_details", "data_element": "PrimaryAgentCommission",
     "doc_value": "0.00%", "sumapp_value": "0.00", "entercalc_value": "0.00%",
     "plan": "N/A", "comparison": "Mismatch"},
    {"category": "agent_details", "data_element": "SecondPrimaryAgentName",
     "doc_value": "NULL", "sumapp_value": "NoData", "entercalc_value": "NoData",
     "plan": "N/A", "comparison": "Match"},
    {"category": "agent_details", "data_element": "SecondPrimaryAgencyName",
     "doc_value": "NULL", "sumapp_value": "NoData", "entercalc_value": "NoData",
     "plan": "N/A", "comparison": "Match"},
    {"category": "agent_details", "data_element": "SecondPrimaryAgencyCommission",
     "doc_value": "NULL", "sumapp_value": "NoData", "entercalc_value": "NoData",
     "plan": "N/A", "comparison": "Match"},
    {"category": "plan_details", "data_element": "PlanName",
     "doc_value": "Blue Select HMO 750/3250", "sumapp_value": "Blue Select HMO 750/3250",
     "entercalc_value": "Blue Select HMO 750/3250", "plan": "BSH-750", "comparison": "Match"},
    {"category": "plan_details", "data_element": "TotalPremium",
     "doc_value": "$4,820.00", "sumapp_value": "4820", "entercalc_value": "$4,820.00",
     "plan": "BSH-750", "comparison": "Mismatch"},
    {"category": "plan_details", "data_element": "EmployeeContribution",
     "doc_value": "$1,200.00", "sumapp_value": "1200.00", "entercalc_value": "$1,200.00",
     "plan": "BSH-750", "comparison": "Mismatch"},
    {"category": "plan_details", "data_element": "EmployerContribution",
     "doc_value": "$3,620.00", "sumapp_value": "3620", "entercalc_value": "$3,620.00",
     "plan": "BSH-750", "comparison": "Mismatch"},
    {"category": "plan_details", "data_element": "DeductibleAmount",
     "doc_value": "$750", "sumapp_value": "$750", "entercalc_value": "$750",
     "plan": "BSH-750", "comparison": "Match"},
    {"category": "plan_details", "data_element": "OutOfPocketMax",
     "doc_value": "$3,250", "sumapp_value": "$3,250", "entercalc_value": "$3,250",
     "plan": "BSH-750", "comparison": "Match"},
]

# In-memory state
CLASSIFIED     = {}   # idx → classification
TICKET_REGISTRY = {}  # data_element → [ticket info]

TICKET_STATUS_MAP = {
    "Open": "Open",
    "In Progress": "In Progress",
    "Resolved": "Resolved",
    "Closed": "Closed",
}

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def get_dashboard_stats(from_date=None, to_date=None):
    from datetime import datetime, timedelta
    
    vals = VALIDATIONS
    if from_date:
        vals = [v for v in vals if v["created_date"][:10] >= from_date]
    if to_date:
        vals = [v for v in vals if v["created_date"][:10] <= to_date]

    # Calculate current period stats
    total_val   = len(vals)
    mismatch    = sum(1 for v in vals if v["status"] == "Mismatch")
    success     = total_val - mismatch
    avg_match   = sum(v["match_rate"] for v in vals) / total_val if total_val else 0
    pending_tx  = sum(1 for t in TRANSACTIONS if not t["validated"])
    validated_tx= sum(1 for t in TRANSACTIONS if t["validated"])
    total_elements_mismatched = sum(v["mismatches"] for v in vals if v["status"] == "Mismatch")
    
    # Calculate prior period for comparison (same duration before the current period)
    if from_date and to_date:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
        period_days = (to_dt - from_dt).days
        prior_to = from_dt - timedelta(days=1)
        prior_from = prior_to - timedelta(days=period_days)
        
        prior_vals = [v for v in VALIDATIONS if prior_from.strftime("%Y-%m-%d") <= v["created_date"][:10] <= prior_to.strftime("%Y-%m-%d")]
        prior_mismatch = sum(1 for v in prior_vals if v["status"] == "Mismatch")
        prior_avg_match = sum(v["match_rate"] for v in prior_vals) / len(prior_vals) if prior_vals else 0
        
        mismatch_vs_prior = mismatch - prior_mismatch
        avg_match_vs_prior = round(avg_match - prior_avg_match, 1)
        period_label = f"{period_days}d"
    else:
        mismatch_vs_prior = 0
        avg_match_vs_prior = 0
        period_label = "60d"
    
    # Calculate aging pending transactions (>5 days old)
    today = datetime.now()
    aging_tx = sum(1 for t in TRANSACTIONS if not t["validated"] and 
                   (today - datetime.strptime(t["request_time"][:10], "%Y-%m-%d")).days > 5)
    
    # Calculate average time to validate (mock calculation based on validated transactions)
    # Using a simple estimate: assume validations happen within 1-3 days
    avg_time_to_validate = 1.4  # Mock value in days
    avg_time_vs_prior = -0.3  # Mock comparison (improvement)

    rally_open   = sum(1 for c in CLASSIFIED.values() if c["type"] == "true_negative"  and c.get("ticket_status","Open") == "Open")
    gitlab_open  = sum(1 for c in CLASSIFIED.values() if c["type"] == "false_negative" and c.get("ticket_status","Open") == "Open")
    rally_closed = sum(1 for c in CLASSIFIED.values() if c["type"] == "true_negative"  and c.get("ticket_status","Open") != "Open")
    gitlab_closed= sum(1 for c in CLASSIFIED.values() if c["type"] == "false_negative" and c.get("ticket_status","Open") != "Open")
    total_doc_issue   = sum(1 for c in CLASSIFIED.values() if c["type"] == "true_negative")
    total_model_issue = sum(1 for c in CLASSIFIED.values() if c["type"] == "false_negative")
    unclassified_count = max(0, mismatch - total_doc_issue - total_model_issue)
    unclassified_open = unclassified_count  # Assume all unclassified are "open"
    unclassified_closed = 0

    return {
        "pending_tx": pending_tx, "validated_tx": validated_tx,
        "total_validations": total_val, "mismatch_count": mismatch,
        "success_count": success,
        "success_pct": round((success / total_val * 100), 1) if total_val else 0,
        "avg_match_rate": round(avg_match, 1),
        "classified_count": len(CLASSIFIED),
        "total_elements": sum(v["total_elements"] for v in vals),
        "total_mismatches": sum(v["mismatches"] for v in vals),
        "rally_open": rally_open, "gitlab_open": gitlab_open,
        "rally_closed": rally_closed, "gitlab_closed": gitlab_closed,
        "total_doc_issue": total_doc_issue, "total_model_issue": total_model_issue,
        "unclassified_count": unclassified_count,
        "unclassified_open": unclassified_open, "unclassified_closed": unclassified_closed,
        # New enhanced metrics
        "mismatch_vs_prior": mismatch_vs_prior,
        "avg_match_vs_prior": avg_match_vs_prior,
        "period_label": period_label,
        "aging_tx": aging_tx,
        "avg_time_to_validate": avg_time_to_validate,
        "avg_time_vs_prior": avg_time_vs_prior,
        "elements_affected": total_elements_mismatched,
        "mismatch_count_for_elements": mismatch,
    }


def get_pending_age_distribution():
    """Calculate age distribution of pending transactions."""
    from datetime import datetime
    
    today = datetime.now()
    pending = [t for t in TRANSACTIONS if not t["validated"]]
    
    age_buckets = {
        "lt_1day": 0,
        "1_2days": 0,
        "3_5days": 0,
        "gt_5days": 0
    }
    
    for t in pending:
        req_date = datetime.strptime(t["request_time"][:10], "%Y-%m-%d")
        age_days = (today - req_date).days
        
        if age_days < 1:
            age_buckets["lt_1day"] += 1
        elif age_days <= 2:
            age_buckets["1_2days"] += 1
        elif age_days <= 5:
            age_buckets["3_5days"] += 1
        else:
            age_buckets["gt_5days"] += 1
    
    total_pending = len(pending)
    sla_exceeded = age_buckets["gt_5days"]
    
    return {
        "total_pending": total_pending,
        "lt_1day": age_buckets["lt_1day"],
        "1_2days": age_buckets["1_2days"],
        "3_5days": age_buckets["3_5days"],
        "gt_5days": age_buckets["gt_5days"],
        "sla_exceeded": sla_exceeded
    }


def get_document_health(from_date=None, to_date=None):
    """Calculate document type health metrics: pass rate, volume, trend, status."""
    vals = VALIDATIONS
    if from_date:
        vals = [v for v in vals if v["created_date"][:10] >= from_date]
    if to_date:
        vals = [v for v in vals if v["created_date"][:10] <= to_date]
    
    # Calculate metrics per document
    doc_metrics = {}
    for v in vals:
        dn = v["doc_name"]
        if dn not in doc_metrics:
            doc_metrics[dn] = {"success": 0, "total": 0, "recent": [], "all_dates": []}
        doc_metrics[dn]["total"] += 1
        doc_metrics[dn]["all_dates"].append(v["created_date"][:10])
        if v["status"] == "Success":
            doc_metrics[dn]["success"] += 1
        # Track recent validations for trend (last 3)
        doc_metrics[dn]["recent"].append(1 if v["status"] == "Success" else 0)
    
    result = []
    for dn, metrics in doc_metrics.items():
        total = metrics["total"]
        success = metrics["success"]
        pass_rate = round((success / total * 100)) if total else 0
        
        # Determine trend based on recent performance
        recent = metrics["recent"][-3:] if len(metrics["recent"]) >= 3 else metrics["recent"]
        if len(recent) >= 2:
            recent_rate = sum(recent) / len(recent)
            trend = "up" if recent_rate >= 0.8 else "down"
        else:
            trend = "neutral"
        
        # Determine status based on pass rate
        if pass_rate >= 90:
            status = "Healthy"
        elif pass_rate >= 60:
            status = "Review"
        else:
            status = "At risk"
        
        result.append({
            "doc_name": dn,
            "pass_rate": pass_rate,
            "volume": total,
            "trend": trend,
            "status": status
        })
    
    # Sort by pass rate descending
    result.sort(key=lambda x: x["pass_rate"], reverse=True)
    return result


def get_doc_summary(from_date=None, to_date=None):
    """Per-document validation counts."""
    vals = VALIDATIONS
    if from_date:
        vals = [v for v in vals if v["created_date"][:10] >= from_date]
    if to_date:
        vals = [v for v in vals if v["created_date"][:10] <= to_date]
    summary = {}
    for v in vals:
        dn = v["doc_name"]
        if dn not in summary:
            summary[dn] = {"success": 0, "mismatch": 0, "total_mismatches": 0, "match_rate_sum": 0}
        if v["status"] == "Mismatch":
            summary[dn]["mismatch"] += 1
        else:
            summary[dn]["success"] += 1
        summary[dn]["total_mismatches"] += v["mismatches"]
        summary[dn]["match_rate_sum"] += v["match_rate"]
    result = []
    for dn, s in summary.items():
        total = s["success"] + s["mismatch"]
        s["doc_name"] = dn
        s["total"] = total
        s["avg_match_rate"] = round(s["match_rate_sum"] / total, 1) if total else 0
        result.append(s)
    return sorted(result, key=lambda x: x["total"], reverse=True)


def get_trend_data(from_date=None, to_date=None):
    trend = {}
    for v in VALIDATIONS:
        day = v["created_date"][:10]
        if from_date and day < from_date: continue
        if to_date   and day > to_date:   continue
        if day not in trend:
            trend[day] = {"success": 0, "mismatch": 0, "pending": 0}
        if v["status"] == "Mismatch": trend[day]["mismatch"] += 1
        else:                          trend[day]["success"]  += 1
    # Add pending from transactions
    for tx in TRANSACTIONS:
        if not tx["validated"]:
            day = tx["request_time"][:10]
            if from_date and day < from_date: continue
            if to_date   and day > to_date:   continue
            trend.setdefault(day, {"success": 0, "mismatch": 0, "pending": 0})
            trend[day]["pending"] += 1
    return sorted(trend.items())


def ai_classify_heuristic(element, doc_val, sumapp_val):
    el = element.lower()
    dv = str(doc_val).lower()
    sv = str(sumapp_val).lower()
    if any(k in el for k in ["date", "period", "effective", "expir"]):
        return {"suggestion": "true_negative", "confidence": "High",
                "reason": f"'{doc_val}' vs '{sumapp_val}' — date format difference (MM/DD/YYYY vs YYYY-MM-DD). Expected behaviour, not a defect."}
    if "$" in str(doc_val) or "%" in str(doc_val):
        return {"suggestion": "true_negative", "confidence": "High",
                "reason": f"'{doc_val}' vs '{sumapp_val}' — currency/percentage formatting. Systems store raw numbers; documents display formatted values."}
    if dv in {"null","nodata","","none","n/a"} or sv in {"null","nodata","","none","n/a"}:
        return {"suggestion": "true_negative", "confidence": "Medium",
                "reason": f"'{doc_val}' vs '{sumapp_val}' — NULL/NoData mapping convention difference, not a data defect."}
    if any(k in el for k in ["commission","rate","premium","amount","contribution"]):
        return {"suggestion": "true_negative", "confidence": "Medium",
                "reason": f"'{doc_val}' vs '{sumapp_val}' — financial field format difference. Verify values are numerically equivalent."}
    return {"suggestion": "false_negative", "confidence": "Medium",
            "reason": f"'{doc_val}' and '{sumapp_val}' appear substantively different. Likely a genuine data defect."}


def get_latest_ticket_for_doc_element(doc_name, element):
    """Find most recent ticket for this doc+element combination."""
    matches = []
    for idx, cls in CLASSIFIED.items():
        if idx < len(EXTRACTION_ROWS):
            row = EXTRACTION_ROWS[idx]
            # Check if same element name
            if row["data_element"] == element:
                matches.append(cls)
    if not matches:
        return None
    return sorted(matches, key=lambda x: x.get("created_at",""), reverse=True)[0]


# ─────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    from_date = request.args.get("from_date", "")
    to_date   = request.args.get("to_date", "")
    if not from_date:
        from_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")
    stats     = get_dashboard_stats(from_date, to_date)
    doc_summary = get_doc_summary(from_date, to_date)
    doc_health = get_document_health(from_date, to_date)
    trend     = get_trend_data(from_date, to_date)
    pending_age = get_pending_age_distribution()
    return render_template("dashboard.html",
                           stats=stats, doc_summary=doc_summary, doc_health=doc_health, trend=trend,
                           pending_age=pending_age,
                           from_date=from_date, to_date=to_date)


@app.route("/transactions")
def transactions():
    q       = request.args.get("q","").strip()
    doc_f   = request.args.get("doc_filter","")
    status_f= request.args.get("status_filter","")  # "pending" | "validated" | ""
    market  = request.args.get("market_segment","")
    page    = int(request.args.get("page", 1))
    per_pg  = int(request.args.get("per_page", 10))
    
    # Date range filtering
    to_date   = request.args.get("to_date") or datetime.now().strftime("%Y-%m-%d")
    from_date = request.args.get("from_date") or (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

    rows = TRANSACTIONS[:]
    
    # Apply date range filter
    rows = [r for r in rows if from_date <= r["request_time"][:10] <= to_date]
    
    if q:
        rows = [r for r in rows if any(q.lower() in str(r[k]).lower()
                for k in ["uuid","case_id","doc_name","group_name","group_number"])]
    if doc_f:
        rows = [r for r in rows if doc_f.lower() in r["doc_name"].lower()]
    if status_f == "pending":
        rows = [r for r in rows if not r["validated"]]
    elif status_f == "validated":
        rows = [r for r in rows if r["validated"]]
    if market:
        rows = [r for r in rows if r["market_segment"] == market]

    total       = len(rows)
    total_pages = max(1, (total + per_pg - 1) // per_pg)
    page        = max(1, min(page, total_pages))
    paged       = rows[(page-1)*per_pg : page*per_pg]

    pending_count   = sum(1 for t in TRANSACTIONS if not t["validated"])
    validated_count = sum(1 for t in TRANSACTIONS if t["validated"])

    return render_template("transactions.html",
                           rows=paged, all_count=total,
                           q=q, doc_filter=doc_f, status_filter=status_f,
                           market_segment=market,
                           page=page, per_page=per_pg, total_pages=total_pages,
                           pending_count=pending_count, validated_count=validated_count,
                           total_count=len(TRANSACTIONS),
                           from_date=from_date, to_date=to_date)


@app.route("/api/validate", methods=["POST"])
def api_validate():
    data = request.get_json(silent=True) or {}
    ids  = data.get("ids", [])
    for tx in TRANSACTIONS:
        if tx["id"] in ids:
            tx["validated"] = True
    return jsonify({"status":"success",
                    "message": f"Validation triggered for {len(ids)} transaction(s).",
                    "redirect": url_for("validations")})


@app.route("/validations")
def validations():
    doc_n  = request.args.get("doc_name","").strip()
    case_i = request.args.get("case_id","").strip()
    grp    = request.args.get("group","").strip()
    from_d = request.args.get("from_date","").strip()
    to_d   = request.args.get("to_date","").strip()
    status = request.args.get("status","").strip()
    market = request.args.get("market_segment","").strip()
    page   = int(request.args.get("page", 1))
    per_pg = int(request.args.get("per_page", 10))

    rows = VALIDATIONS[:]
    if doc_n:  rows = [r for r in rows if doc_n.lower()  in r["doc_name"].lower()]
    if case_i: rows = [r for r in rows if case_i.lower() in r["case_id"].lower()]
    if grp:    rows = [r for r in rows if grp.lower()    in r["group_number"].lower()]
    if status: rows = [r for r in rows if r["status"] == status]
    if market: rows = [r for r in rows if r["market_segment"] == market]

    total       = len(rows)
    total_pages = max(1, (total + per_pg - 1) // per_pg)
    page        = max(1, min(page, total_pages))
    paged       = rows[(page-1)*per_pg : page*per_pg]

    return render_template("validations.html", rows=paged,
                           doc_name=doc_n, case_id=case_i, group=grp,
                           from_date=from_d, to_date=to_d,
                           status_filter=status, market_segment=market,
                           page=page, per_page=per_pg, total_pages=total_pages,
                           total=len(VALIDATIONS), filtered_total=total)


@app.route("/api/export/validations")
def export_validations():
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Incident ID","Transaction ID","Document Name","Case ID","Group Number",
                "Market Segment","Group Type","Rate Type","Contract Type","Census Tiers",
                "Status","Match Rate","Mismatches","Total Elements","Created Date"])
    for v in VALIDATIONS:
        w.writerow([v["incident_id"], v["transaction_id"], v["doc_name"],
                    v["case_id"], v["group_number"],
                    v["market_segment"], v["group_type"], v["rate_type"],
                    v["contract_type"], v["census_tiers"],
                    v["status"], v["match_rate"], v["mismatches"],
                    v["total_elements"], v["created_date"]])
    out.seek(0)
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment; filename=validations_export.csv"})


@app.route("/api/export/detail/<path:incident_id>")
def export_detail(incident_id):
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Category","Data Element","Doc Value","SumApp Value",
                "EnterCalc Value","Plan","Comparison","Classification","Ticket ID","Ticket Status"])
    for i, r in enumerate(EXTRACTION_ROWS):
        cls = CLASSIFIED.get(i)
        w.writerow([r["category"], r["data_element"], r["doc_value"],
                    r["sumapp_value"], r["entercalc_value"], r["plan"],
                    r["comparison"], cls["type"] if cls else "",
                    cls["ticket_id"] if cls else "",
                    cls.get("ticket_status","") if cls else ""])
    out.seek(0)
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=detail_{incident_id[:20]}.csv"})


@app.route("/validations/<path:incident_id>")
def validation_detail(incident_id):
    record = next((v for v in VALIDATIONS if v["incident_id"] == incident_id), None)
    if not record:
        flash("Record not found.", "error")
        return redirect(url_for("validations"))
    rows = [dict(r, idx=i, classified=CLASSIFIED.get(i))
            for i, r in enumerate(EXTRACTION_ROWS)]
    mismatch_count   = sum(1 for r in EXTRACTION_ROWS if r["comparison"] == "Mismatch")
    match_count      = sum(1 for r in EXTRACTION_ROWS if r["comparison"] == "Match")
    classified_count = len(CLASSIFIED)
    plans = list(set(r["plan"] for r in EXTRACTION_ROWS if r["plan"] != "N/A"))
    return render_template("detail.html",
                           record=record, rows=rows,
                           mismatch_count=mismatch_count,
                           match_count=match_count,
                           classified_count=classified_count,
                           plans=plans)


@app.route("/api/ai-suggest", methods=["POST"])
def api_ai_suggest():
    data       = request.get_json(silent=True) or {}
    element    = data.get("element","")
    doc_val    = data.get("doc_value","")
    sumapp_val = data.get("sumapp_value","")
    doc_name   = data.get("doc_name","")

    result = ai_classify_heuristic(element, doc_val, sumapp_val)
    result["similar_tickets"] = TICKET_REGISTRY.get(element, [])

    # Latest ticket for same doc+element
    latest = get_latest_ticket_for_doc_element(doc_name, element)
    result["latest_ticket"] = latest
    return jsonify(result)


@app.route("/api/classify", methods=["POST"])
def api_classify():
    data       = request.get_json(silent=True) or {}
    idx        = data.get("idx")
    cls_type   = data.get("type")
    title      = data.get("title","")
    desc       = data.get("description","")
    assignee   = data.get("assignee","")
    severity   = data.get("severity","Medium")
    incident   = data.get("incident_id","")
    ref_ticket = data.get("ref_ticket","")
    ticket_ref_note = data.get("ticket_ref_note","")

    if idx is None or cls_type not in ("true_negative","false_negative"):
        return jsonify({"status":"error","message":"Invalid input"}), 400

    prefix    = "US-" if cls_type == "true_negative" else "BUG-"
    ticket_id = ref_ticket if ref_ticket else f"{prefix}{random.randint(10000,99999)}"

    CLASSIFIED[idx] = {
        "type": cls_type, "ticket_id": ticket_id, "title": title,
        "description": desc, "assignee": assignee, "severity": severity,
        "incident_id": incident, "is_ref": bool(ref_ticket),
        "ticket_status": "Open",
        "ticket_ref_note": ticket_ref_note,
        "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
    }

    element = EXTRACTION_ROWS[idx]["data_element"] if idx < len(EXTRACTION_ROWS) else ""
    if element and not ref_ticket:
        TICKET_REGISTRY.setdefault(element, []).append({
            "ticket_id": ticket_id, "type": cls_type, "title": title,
            "incident_id": incident, "created_at": CLASSIFIED[idx]["created_at"],
            "ticket_status": "Open",
        })

    system = "Rally" if cls_type == "true_negative" else "GitLab"
    ticket_url = (f"https://rally.company.com/stories/{ticket_id}"
                  if cls_type == "true_negative"
                  else f"https://gitlab.company.com/issues/{ticket_id}")

    return jsonify({"status":"success","ticket_id":ticket_id,"ticket_url":ticket_url,
                    "system":system,"is_ref":bool(ref_ticket),
                    "message": (f"Linked to {ticket_id}." if ref_ticket
                                else f"{'Document Issue' if cls_type=='true_negative' else 'Model Issue'} {ticket_id} created in {system}.")})


@app.route("/api/sync-tickets", methods=["POST"])
def api_sync_tickets():
    """Mock sync — randomly update some ticket statuses."""
    updated = 0
    statuses = ["Open","In Progress","Resolved"]
    for idx, cls in CLASSIFIED.items():
        if cls.get("ticket_status","Open") == "Open":
            # 30% chance of progressing
            if random.random() < 0.3:
                cls["ticket_status"] = "In Progress"
                updated += 1
        elif cls.get("ticket_status") == "In Progress":
            if random.random() < 0.2:
                cls["ticket_status"] = "Resolved"
                updated += 1
        # Sync in registry too
        element = EXTRACTION_ROWS[idx]["data_element"] if idx < len(EXTRACTION_ROWS) else ""
        for t in TICKET_REGISTRY.get(element, []):
            if t["ticket_id"] == cls["ticket_id"]:
                t["ticket_status"] = cls["ticket_status"]
    return jsonify({"status":"success","updated":updated,
                    "message":f"Synced {len(CLASSIFIED)} tickets. {updated} status update(s)."})


@app.route("/api/ticket-status/<ticket_id>", methods=["POST"])
def api_ticket_status(ticket_id):
    data = request.get_json(silent=True) or {}
    new_status = data.get("status","Open")
    for idx, cls in CLASSIFIED.items():
        if cls["ticket_id"] == ticket_id:
            cls["ticket_status"] = new_status
    return jsonify({"status":"success","ticket_id":ticket_id,"new_status":new_status})


@app.route("/mismatch-analysis")
def mismatch_analysis():
    # Per-element mismatch breakdown
    element_counts = {}
    for r in EXTRACTION_ROWS:
        if r["comparison"] == "Mismatch":
            el = r["data_element"]
            element_counts[el] = element_counts.get(el, 0) + 1

    # Per-category breakdown
    category_counts = {}
    for r in EXTRACTION_ROWS:
        if r["comparison"] == "Mismatch":
            cat = r["category"]
            category_counts[cat] = category_counts.get(cat, 0) + 1

    # Classification breakdown
    doc_issue    = sum(1 for c in CLASSIFIED.values() if c["type"] == "true_negative")
    model_issue  = sum(1 for c in CLASSIFIED.values() if c["type"] == "false_negative")
    unclassified = sum(1 for r in EXTRACTION_ROWS if r["comparison"] == "Mismatch") - len(CLASSIFIED)

    # Status breakdown
    status_counts = {"Open":0,"In Progress":0,"Resolved":0,"Closed":0}
    for c in CLASSIFIED.values():
        s = c.get("ticket_status","Open")
        status_counts[s] = status_counts.get(s, 0) + 1

    # Mismatch by document
    doc_mismatches = {}
    for v in VALIDATIONS:
        if v["mismatches"] > 0:
            dn = v["doc_name"]
            doc_mismatches[dn] = doc_mismatches.get(dn, 0) + v["mismatches"]

    # By market segment
    seg_stats = {}
    for v in VALIDATIONS:
        seg = v["market_segment"]
        if seg not in seg_stats:
            seg_stats[seg] = {"total":0,"mismatches":0}
        seg_stats[seg]["total"] += 1
        if v["status"] == "Mismatch":
            seg_stats[seg]["mismatches"] += 1

    return render_template("mismatch_analysis.html",
                           element_counts=element_counts,
                           category_counts=category_counts,
                           doc_issue=doc_issue, model_issue=model_issue,
                           unclassified=max(0, unclassified),
                           status_counts=status_counts,
                           doc_mismatches=doc_mismatches,
                           seg_stats=seg_stats,
                           classified=CLASSIFIED,
                           extraction_rows=EXTRACTION_ROWS)


@app.route("/api/ai-qa", methods=["POST"])
def api_ai_qa():
    data     = request.get_json(silent=True) or {}
    question = data.get("question","").lower().strip()

    # Context-aware heuristic Q&A
    total_val   = len(VALIDATIONS)
    mismatches  = sum(1 for v in VALIDATIONS if v["status"] == "Mismatch")
    pending_tx  = sum(1 for t in TRANSACTIONS if not t["validated"])
    classified  = len(CLASSIFIED)

    # Keyword routing
    if any(w in question for w in ["mismatch","mismatches","how many mismatch"]):
        answer = (f"There are currently **{mismatches}** validation records with mismatches "
                  f"out of **{total_val}** total validations. "
                  f"The most common mismatch patterns involve date formats (MM/DD/YYYY vs YYYY-MM-DD), "
                  f"currency formatting ($X.XX vs raw numbers), and NULL/NoData equivalences.")

    elif any(w in question for w in ["pending","not validated","unvalidated"]):
        answer = (f"There are **{pending_tx}** transactions currently pending validation. "
                  f"Most are from the most recent batch submitted on 2026-03-29. "
                  f"Select them on the Transactions page and click Validate Selected.")

    elif any(w in question for w in ["rally","document issue","true negative"]):
        doc_issues = sum(1 for c in CLASSIFIED.values() if c["type"]=="true_negative")
        open_r     = sum(1 for c in CLASSIFIED.values() if c["type"]=="true_negative" and c.get("ticket_status","Open")=="Open")
        answer = (f"There are **{doc_issues}** Document Issues (Rally User Stories) classified. "
                  f"**{open_r}** are currently Open. "
                  f"Document issues are typically caused by formatting differences that are expected behaviour — "
                  f"such as date format or currency display differences.")

    elif any(w in question for w in ["gitlab","model issue","false negative","bug"]):
        model_issues = sum(1 for c in CLASSIFIED.values() if c["type"]=="false_negative")
        open_g       = sum(1 for c in CLASSIFIED.values() if c["type"]=="false_negative" and c.get("ticket_status","Open")=="Open")
        answer = (f"There are **{model_issues}** Model Issues (GitLab Bugs) raised. "
                  f"**{open_g}** are currently Open. "
                  f"Model issues represent genuine data defects where values differ substantively, "
                  f"requiring developer investigation.")

    elif any(w in question for w in ["large group","small group","market segment"]):
        lg = sum(1 for v in VALIDATIONS if v["market_segment"]=="Large Group")
        sg = sum(1 for v in VALIDATIONS if v["market_segment"]=="Small Group")
        answer = (f"Validation breakdown by market segment: "
                  f"**Large Group**: {lg} records, **Small Group**: {sg} records. "
                  f"Large group documents tend to have more complex plan structures with multiple tiers.")

    elif any(w in question for w in ["match rate","accuracy","rate"]):
        avg = sum(v["match_rate"] for v in VALIDATIONS) / len(VALIDATIONS)
        answer = (f"The overall average match rate across all validations is **{avg:.1f}%**. "
                  f"Records with 100% match rate: {sum(1 for v in VALIDATIONS if v['match_rate']==100)}. "
                  f"Records below 90%: {sum(1 for v in VALIDATIONS if v['match_rate']<90)}.")

    elif any(w in question for w in ["classify","classified","ticket"]):
        answer = (f"**{classified}** mismatches have been classified this session. "
                  f"To classify: open any validation detail, find a Mismatch row, and click the Classify button. "
                  f"The AI Assist will suggest Document Issue (Rally) or Model Issue (GitLab) based on the field type.")

    elif any(w in question for w in ["effective_date","date","format"]):
        answer = ("Date format mismatches (e.g. '06/01/2026' vs '2026-06-01') are **Document Issues** — "
                  "they represent the same value formatted differently by different systems. "
                  "These should be classified as Document Issue and assigned a Rally User Story for normalisation tracking.")

    elif any(w in question for w in ["commission","premium","currency","dollar","$","%"]):
        answer = ("Currency and percentage formatting mismatches (e.g. '$4,820.00' vs '4820') are **Document Issues**. "
                  "The underlying data is numerically equivalent — the difference is purely presentational. "
                  "These are expected and should be tracked as User Stories in Rally.")

    elif any(w in question for w in ["help","what can","guide","how"]):
        answer = ("I can help you with:\n"
                  "• **Mismatch counts** — 'How many mismatches are there?'\n"
                  "• **Pending transactions** — 'How many transactions are pending?'\n"
                  "• **Ticket status** — 'How many Rally tickets are open?'\n"
                  "• **Classification guidance** — 'How should I classify date format mismatches?'\n"
                  "• **Market segment breakdown** — 'Show Large Group vs Small Group stats'\n"
                  "• **Match rate analysis** — 'What is the average match rate?'")
    else:
        answer = (f"Based on current data: **{total_val}** validations processed, "
                  f"**{mismatches}** with mismatches, **{pending_tx}** transactions pending. "
                  f"Try asking about specific topics like mismatches, Rally tickets, classification guidance, "
                  f"or market segment breakdowns.")

    return jsonify({"answer": answer, "question": question})


if __name__ == "__main__":
    app.run(debug=True, port=5100)
