"""
Customer.io App API Client

Checks segment membership (buyer segments per funnel) and customer attributes.
Uses CIO App API with Bearer token auth.

CRITICAL: EU datacenter — all requests go to api-eu.customer.io, NOT api.customer.io.

Segment IDs (buyer segments):
  - hypnose:    168
  - meditation: 172
  - lifecoach:  170
"""

import os
import time
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

CIO_APP_API_KEY = os.environ.get("CIO_APP_API_KEY", "")
CIO_API_BASE = os.environ.get("CIO_API_BASE", "https://api-eu.customer.io/v1")

# Buyer segment IDs per funnel
BUYER_SEGMENTS: dict[str, int] = {
    "hypnose": 168,
    "meditation": 172,
    "lifecoach": 170,
}

# Cache: {segment_id: (timestamp, set_of_emails)}
_segment_cache: dict[int, tuple[float, set[str]]] = {}
_CACHE_TTL_SECONDS = 3600  # 1 hour


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {CIO_APP_API_KEY}",
        "Content-Type": "application/json",
    }


def _ensure_credentials() -> None:
    if not CIO_APP_API_KEY:
        raise EnvironmentError("CIO_APP_API_KEY must be set")


async def _fetch_segment_members(segment_id: int) -> set[str]:
    """
    Fetch all member emails for a segment, paginating through all pages.
    Results are cached for 1 hour.

    Returns a set of lowercase email addresses.
    """
    # Check cache first
    cached = _segment_cache.get(segment_id)
    if cached:
        ts, emails = cached
        if time.time() - ts < _CACHE_TTL_SECONDS:
            logger.debug("CIO segment %d: cache hit (%d members)", segment_id, len(emails))
            return emails

    _ensure_credentials()
    emails: set[str] = set()
    cursor: str | None = None

    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            params: dict[str, Any] = {"limit": 100}
            if cursor:
                params["start"] = cursor

            response = await client.get(
                f"{CIO_API_BASE}/segments/{segment_id}/membership",
                headers=_headers(),
                params=params,
            )

            if response.status_code != 200:
                logger.error(
                    "CIO segment %d membership fetch failed: %s %s",
                    segment_id, response.status_code, response.text[:500],
                )
                response.raise_for_status()

            data = response.json()
            identifiers = data.get("identifiers", [])

            for member in identifiers:
                email = member.get("email", "")
                if email:
                    emails.add(email.lower())

            # Pagination: CIO uses cursor-based pagination
            next_cursor = data.get("next", "")
            if not next_cursor or not identifiers:
                break
            cursor = next_cursor

    # Update cache
    _segment_cache[segment_id] = (time.time(), emails)
    logger.info("CIO segment %d: fetched %d members (cached for %ds)", segment_id, len(emails), _CACHE_TTL_SECONDS)
    return emails


async def is_buyer_in_funnel(email: str, funnel: str) -> bool:
    """
    Check if a person is in the buyer segment for a specific funnel.

    Args:
        email: Customer email address
        funnel: One of 'hypnose', 'meditation', 'lifecoach'

    Returns:
        True if email is a member of the buyer segment for that funnel.
    """
    segment_id = BUYER_SEGMENTS.get(funnel.lower())
    if segment_id is None:
        logger.warning("CIO: unknown funnel '%s' — valid: %s", funnel, list(BUYER_SEGMENTS.keys()))
        return False

    members = await _fetch_segment_members(segment_id)
    return email.lower() in members


async def get_purchased_funnels(email: str) -> list[str]:
    """
    Return list of funnels the person has purchased.

    Checks all 3 buyer segments and returns matching funnel names.
    Example: ["hypnose", "meditation"]
    """
    _ensure_credentials()
    purchased: list[str] = []

    for funnel, segment_id in BUYER_SEGMENTS.items():
        members = await _fetch_segment_members(segment_id)
        if email.lower() in members:
            purchased.append(funnel)

    logger.debug("CIO: %s purchased funnels: %s", email, purchased or "none")
    return purchased


async def is_unsubscribed(cio_id: str) -> bool:
    """
    Check if a customer is unsubscribed via their attributes.

    Args:
        cio_id: Customer.io customer ID

    Returns:
        True if the customer has unsubscribed.
    """
    attrs = await get_customer_attributes(cio_id)
    if attrs is None:
        return False

    unsubscribed = attrs.get("unsubscribed", False)
    # CIO may return string "true" or boolean True
    if isinstance(unsubscribed, str):
        return unsubscribed.lower() in ("true", "1", "yes")
    return bool(unsubscribed)


async def get_customer_attributes(cio_id: str) -> dict | None:
    """
    Fetch full customer attributes from Customer.io.

    Returns dict with email, first_name, phone, tags, unsubscribed status,
    or None if customer not found.
    """
    if not cio_id:
        return None

    _ensure_credentials()

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{CIO_API_BASE}/customers/{cio_id}/attributes",
            headers=_headers(),
        )

    if response.status_code == 404:
        logger.info("CIO: customer %s not found", cio_id)
        return None

    if response.status_code != 200:
        logger.error(
            "CIO: attributes fetch failed for %s: %s %s",
            cio_id, response.status_code, response.text[:500],
        )
        response.raise_for_status()

    data = response.json()
    # CIO wraps attributes under 'customer' key
    customer = data.get("customer", data)
    attributes = customer.get("attributes", customer)

    return {
        "email": attributes.get("email", ""),
        "first_name": attributes.get("first_name", ""),
        "phone": attributes.get("phone", ""),
        "tags": attributes.get("tags", []),
        "unsubscribed": attributes.get("unsubscribed", False),
        "cio_id": cio_id,
    }


def clear_segment_cache() -> None:
    """Clear the in-memory segment membership cache (useful for testing)."""
    _segment_cache.clear()
    logger.debug("CIO: segment cache cleared")
