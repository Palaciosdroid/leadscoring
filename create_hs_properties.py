"""Create lead_* custom properties in HubSpot and update scoring code to match."""
import os
import urllib.request
import urllib.error
import json

TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
if not TOKEN:
    raise SystemExit("HUBSPOT_ACCESS_TOKEN env var is required — set it before running.")
BASE = "https://api.hubapi.com/crm/v3/properties/contacts"

PROPERTIES = [
    {
        "name": "lead_engagement_score",
        "label": "Lead Engagement Score",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Behaviour-based engagement score (0-100)",
    },
    {
        "name": "lead_combined_score",
        "label": "Lead Combined Score",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Combined engagement + AI score (0-100)",
    },
    {
        "name": "lead_ai_score",
        "label": "Lead AI Score",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "AI predictive score (0-100), null if model not ready",
    },
    {
        "name": "lead_tier",
        "label": "Lead Tier",
        "type": "enumeration",
        "fieldType": "select",
        "groupName": "contactinformation",
        "description": "Scoring tier: Hot / Warm / Cold / Disqualified",
        "options": [
            {"label": "Hot",          "value": "1_hot",          "displayOrder": 1},
            {"label": "Warm",         "value": "2_warm",         "displayOrder": 2},
            {"label": "Cold",         "value": "3_cold",         "displayOrder": 3},
            {"label": "Disqualified", "value": "4_disqualified", "displayOrder": 4},
        ],
    },
    {
        "name": "lead_interest_category",
        "label": "Lead Interest Category",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "Detected product interest category from browsing behaviour",
    },
    {
        "name": "lead_score_updated_at",
        "label": "Lead Score Updated At",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "ISO 8601 timestamp of last score update",
    },
    {
        "name": "lead_funnel_source",
        "label": "Lead Funnel Source",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "CIO campaign / funnel that generated this lead (email subject or campaign ID)",
    },
    {
        "name": "lead_score_version",
        "label": "Lead Score Version",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "Scoring engine version",
    },
    {
        "name": "lead_pause_until",
        "label": "Lead Pause Until",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "ISO 8601 timestamp until which the lead is paused from the Aircall dialer",
    },
    {
        "name": "lead_no_answer_streak",
        "label": "Lead No-Answer Streak",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Consecutive no-answer calls; reset to 0 when the lead is reached",
    },
    {
        "name": "lead_no_answer_cycles",
        "label": "Lead No-Answer Cycles",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Completed 2-month no-answer pause cycles; at 2 the lead is removed",
    },
    {
        "name": "lead_dialer_removed",
        "label": "Lead Dialer Removed",
        "type": "enumeration",
        "fieldType": "booleancheckbox",
        "groupName": "contactinformation",
        "description": "True when the lead is permanently removed from the dialer (cycle cap or wrong number)",
        "options": [
            {"label": "true",  "value": "true",  "displayOrder": 0},
            {"label": "false", "value": "false", "displayOrder": 1},
        ],
    },
    {
        "name": "lead_phone_status",
        "label": "Lead Phone Status",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "Phone validation result: valid / corrected / invalid",
    },
    {
        "name": "lead_phone_dnc",
        "label": "Lead Phone Do-Not-Call",
        "type": "enumeration",
        "fieldType": "booleancheckbox",
        "groupName": "contactinformation",
        "description": "True when the lead asked not to be called by phone (independent of email opt-out)",
        "options": [
            {"label": "true",  "value": "true",  "displayOrder": 0},
            {"label": "false", "value": "false", "displayOrder": 1},
        ],
    },
    {
        "name": "lead_eig_budget",
        "label": "Lead Eignungscheck Budget",
        "type": "enumeration",
        "fieldType": "select",
        "groupName": "contactinformation",
        "description": "Tally Eignungscheck: self-reported budget range",
        "options": [
            {"label": "Unter 2000",  "value": "unter_2000", "displayOrder": 1},
            {"label": "2000 - 4000", "value": "2000_4000",  "displayOrder": 2},
            {"label": "4000 - 6000", "value": "4000_6000",  "displayOrder": 3},
            {"label": "6000 - 8000", "value": "6000_8000",  "displayOrder": 4},
        ],
    },
    {
        "name": "lead_eig_interest",
        "label": "Lead Eignungscheck Interest",
        "type": "enumeration",
        "fieldType": "select",
        "groupName": "contactinformation",
        "description": "Tally Eignungscheck: self-reported interest level",
        "options": [
            {"label": "Gar nicht interessiert",      "value": "keines",           "displayOrder": 1},
            {"label": "Grundsaetzlich interessiert", "value": "grundsaetzlich",   "displayOrder": 2},
            {"label": "Richtiger naechster Schritt", "value": "naechster_schritt", "displayOrder": 3},
        ],
    },
    {
        "name": "lead_eig_consult",
        "label": "Lead Eignungscheck Consult",
        "type": "enumeration",
        "fieldType": "booleancheckbox",
        "groupName": "contactinformation",
        "description": "Tally Eignungscheck: wants a consultation ('Ja, gerne!')",
        "options": [
            {"label": "true",  "value": "true",  "displayOrder": 0},
            {"label": "false", "value": "false", "displayOrder": 1},
        ],
    },
    {
        "name": "lead_eig_goal",
        "label": "Lead Eignungscheck Goal",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "Tally Eignungscheck: free-text goal (conversation opener for Aircall card)",
    },
    {
        "name": "lead_eig_score",
        "label": "Lead Eignungscheck Score",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Tally Eignungscheck: self-assessment score shown on the Aircall card",
    },
    {
        "name": "lead_points",
        "label": "Lead Points",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Transparent point-system score (shadow); targets HubSpot Deal Won",
    },
]


def create_property(prop):
    data = json.dumps(prop).encode()
    req = urllib.request.Request(
        BASE,
        data=data,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            print(f"  OK: {result['name']}")
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        msg = body.get("message", "unknown error")
        if "already exists" in msg.lower() or "PROPERTY_ALREADY_EXISTS" in str(body):
            print(f"  SKIP (already exists): {prop['name']}")
        else:
            print(f"  ERROR {e.code}: {msg}")


if __name__ == "__main__":
    print("Creating HubSpot custom properties...\n")
    for p in PROPERTIES:
        print(f"- {p['name']}")
        create_property(p)
    print("\nDone.")
