"""
Zoom API Client — Server-to-Server OAuth

Handles:
- OAuth token management (auto-refresh)
- Fetching recording download URLs after a meeting ends
- Fetching meeting participants (to get attendee emails for HubSpot matching)

Env vars required:
    ZOOM_ACCOUNT_ID       — from Zoom Server-to-Server OAuth app
    ZOOM_CLIENT_ID        — from Zoom Server-to-Server OAuth app
    ZOOM_CLIENT_SECRET    — from Zoom Server-to-Server OAuth app
    ZOOM_WEBHOOK_SECRET   — optional, for webhook signature verification
"""

import hashlib
import hmac
import logging
import os
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

ZOOM_ACCOUNT_ID    = os.environ.get("ZOOM_ACCOUNT_ID", "")
ZOOM_CLIENT_ID     = os.environ.get("ZOOM_CLIENT_ID", "")
ZOOM_CLIENT_SECRET = os.environ.get("ZOOM_CLIENT_SECRET", "")
ZOOM_WEBHOOK_SECRET = os.environ.get("ZOOM_WEBHOOK_SECRET", "")

_TOKEN_CACHE: dict[str, Any] = {"token": None, "expires_at": 0}


async def _get_access_token() -> str:
    """Get a valid access token, refreshing if expired."""
    now = time.time()
    if _TOKEN_CACHE["token"] and _TOKEN_CACHE["expires_at"] > now + 60:
        return _TOKEN_CACHE["token"]

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(
            "https://zoom.us/oauth/token",
            params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
            auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        )
        r.raise_for_status()
        data = r.json()
        _TOKEN_CACHE["token"] = data["access_token"]
        _TOKEN_CACHE["expires_at"] = now + data.get("expires_in", 3600)
        logger.debug("Zoom: refreshed access token (expires in %ds)", data.get("expires_in"))
        return _TOKEN_CACHE["token"]


async def get_recording_files(meeting_uuid: str) -> list[dict]:
    """
    Fetch recording files for a completed meeting.
    Returns list of recording file dicts with download_url, file_type, etc.

    The meeting_uuid from the webhook payload must be double-URL-encoded
    if it contains '/' or '+' characters.
    """
    token = await _get_access_token()

    # Double-encode UUID if it contains special chars (Zoom API requirement)
    encoded_uuid = meeting_uuid
    if "/" in meeting_uuid or "+" in meeting_uuid:
        from urllib.parse import quote
        encoded_uuid = quote(quote(meeting_uuid, safe=""), safe="")

    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"https://api.zoom.us/v2/meetings/{encoded_uuid}/recordings",
            headers={"Authorization": f"Bearer {token}"},
        )
        if r.status_code == 404:
            logger.warning("Zoom: no recordings found for meeting %s", meeting_uuid)
            return []
        r.raise_for_status()
        data = r.json()

    files = data.get("recording_files", [])
    logger.info("Zoom: found %d recording file(s) for meeting %s", len(files), meeting_uuid)
    return files


async def get_vtt_url(meeting_uuid: str) -> str | None:
    """
    Get the VTT (WebVTT transcript) download URL for a completed meeting.
    Zoom generates VTT automatically when cloud recording + transcription is enabled.

    Returns download_url string or None if not available.
    """
    files = await get_recording_files(meeting_uuid)

    for f in files:
        if f.get("file_type", "").upper() == "TRANSCRIPT" or \
           f.get("file_extension", "").lower() == "vtt":
            url = f.get("download_url")
            if url:
                logger.info("Zoom: found VTT transcript file for meeting %s", meeting_uuid)
                return url

    logger.warning(
        "Zoom: no VTT file found for meeting %s — "
        "check that cloud recording + transcription is enabled in Zoom settings",
        meeting_uuid,
    )
    return None


async def download_recording(download_url: str) -> bytes:
    """
    Download a Zoom recording file.
    Zoom requires the access token as query param for direct downloads.
    """
    token = await _get_access_token()

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        r = await client.get(
            download_url,
            params={"access_token": token},
        )
        r.raise_for_status()
        logger.info("Zoom: downloaded recording (%d bytes)", len(r.content))
        return r.content


async def get_meeting_participants(meeting_id: str) -> list[dict]:
    """
    Fetch participant list for a meeting — used to get attendee emails
    for matching against HubSpot contacts.

    Returns list of participant dicts with name, user_email, join_time, etc.
    """
    token = await _get_access_token()

    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"https://api.zoom.us/v2/report/meetings/{meeting_id}/participants",
            headers={"Authorization": f"Bearer {token}"},
            params={"page_size": 50},
        )
        if r.status_code == 404:
            logger.warning("Zoom: no participants found for meeting %s", meeting_id)
            return []
        r.raise_for_status()
        data = r.json()

    participants = data.get("participants", [])
    logger.info("Zoom: found %d participant(s) for meeting %s", len(participants), meeting_id)
    return participants


async def get_lead_email_from_meeting(meeting_id: str, host_email: str | None = None) -> str | None:
    """
    Identify the lead's email from meeting participants.
    Assumes 2-person call: host (Kevin) + lead.
    Excludes the host email to find the lead.

    Returns lead email or None if not found.
    """
    participants = await get_meeting_participants(meeting_id)

    emails = [
        p.get("user_email", "").strip().lower()
        for p in participants
        if p.get("user_email")
    ]

    # Remove duplicates and empty
    emails = list(dict.fromkeys(e for e in emails if e))

    if not emails:
        logger.warning("Zoom: no participant emails found for meeting %s", meeting_id)
        return None

    # If host email known, exclude it to find the lead
    if host_email:
        host_lower = host_email.strip().lower()
        lead_emails = [e for e in emails if e != host_lower]
        if lead_emails:
            return lead_emails[0]

    # Single participant (other than potential duplicates) — return first non-host
    # Fallback: return first email found
    return emails[0] if emails else None


def verify_webhook_signature(payload: bytes, timestamp: str, signature: str) -> bool:
    """
    Verify Zoom webhook signature (v2 verification).
    Zoom sends: x-zm-request-timestamp + x-zm-signature headers.
    """
    if not ZOOM_WEBHOOK_SECRET:
        return True  # Skip verification if no secret configured

    message = f"v0:{timestamp}:{payload.decode('utf-8')}"
    expected = "v0=" + hmac.new(
        ZOOM_WEBHOOK_SECRET.encode(),
        message.encode(),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, signature)
