"""
Unsubscribe Handler — removes contacts from lists and Power Dialer when they unsubscribe.

When a contact unsubscribes:
1. Remove from all active HubSpot lists
2. Remove from Aircall Power Dialer queue
3. Mark as "do not contact" (this is done automatically by batch scorer via -50 malus)
"""

import logging
from typing import Any

from integrations.aircall import remove_from_power_dialer
from integrations.hubspot import remove_from_lists

logger = logging.getLogger(__name__)


async def handle_unsubscribe(
    contact_id: str,
    email: str,
    phone: str = "",
) -> dict[str, Any]:
    """
    Handle a contact unsubscribe event:
    1. Remove from HubSpot lists
    2. Remove from Aircall Power Dialer

    Returns status dict with results.
    """
    logger.info("unsubscribe_handler: processing unsubscribe for %s", email)

    results = {
        "email": email,
        "contact_id": contact_id,
        "removed_from_lists": False,
        "removed_from_aircall": False,
        "errors": [],
    }

    # Step 1: Remove from HubSpot lists
    try:
        removed = await remove_from_lists(contact_id)
        results["removed_from_lists"] = removed
        if removed:
            logger.info("unsubscribe_handler: removed %s from HubSpot lists", email)
        else:
            logger.warning("unsubscribe_handler: failed to remove %s from HubSpot lists", email)
    except Exception as e:
        logger.error("unsubscribe_handler: HubSpot removal failed for %s: %s", email, e)
        results["errors"].append(f"HubSpot: {str(e)}")

    # Step 2: Remove from Aircall Power Dialer (only if phone available)
    if phone:
        try:
            removed = await remove_from_power_dialer(phone)
            results["removed_from_aircall"] = removed
            if removed:
                logger.info("unsubscribe_handler: removed %s from Aircall Power Dialer", phone)
            else:
                logger.warning("unsubscribe_handler: failed to remove %s from Aircall", phone)
        except Exception as e:
            logger.error("unsubscribe_handler: Aircall removal failed for %s: %s", phone, e)
            results["errors"].append(f"Aircall: {str(e)}")
    else:
        logger.debug("unsubscribe_handler: no phone number for %s — skipping Aircall removal", email)

    logger.info(
        "unsubscribe_handler: completed for %s (lists=%s, aircall=%s, errors=%d)",
        email,
        results["removed_from_lists"],
        results["removed_from_aircall"],
        len(results["errors"]),
    )

    return results
