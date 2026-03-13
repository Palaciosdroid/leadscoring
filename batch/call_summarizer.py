"""
Call Summarizer — Zoom VTT → Claude Haiku Summary → HubSpot Note

Flow:
  1. Download VTT file (Zoom's auto-generated transcript — free, instant)
  2. Parse VTT → clean plain text with speaker lines
  3. Summarize via Claude Haiku — structured DE output for HubSpot Note

No Whisper needed — Zoom transcribes automatically at no extra cost.

Env vars required:
    ANTHROPIC_API_KEY  — for Claude Haiku summary
"""

import logging
import os
import re

import httpx

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

_SUMMARY_SYSTEM_PROMPT = """Du bist ein Assistent der Verkaufsgespräche für Gabriel Palacios (Hypnose, Meditation, Lifecoaching) analysiert.
Erstelle eine strukturierte Gesprächs-Zusammenfassung für den Closer Kevin.
Sei präzise, praxisorientiert und auf Deutsch.
Keine Floskeln, nur das Wesentliche."""

_SUMMARY_USER_TEMPLATE = """Analysiere dieses Verkaufsgespräch-Transkript und erstelle eine strukturierte Zusammenfassung.

TRANSKRIPT:
{transcript}

Erstelle eine Zusammenfassung in genau diesem Format:

📞 Gesprächs-Zusammenfassung
──────────────────────────
🎯 Hauptanliegen: [Was will der Lead konkret? 1-2 Sätze]
💬 Wichtigste Aussagen:
• [Aussage 1]
• [Aussage 2]
• [Aussage 3 wenn relevant]

🚧 Einwände / Bedenken: [Was hält ihn zurück? Oder "Keine Einwände genannt"]
💰 Kaufbereitschaft: [HOCH / MITTEL / NIEDRIG] — [kurze Begründung]
➡️ Empfohlener nächster Schritt: [Was sollte Kevin jetzt tun?]
──────────────────────────
⏱️ Gesprächsdauer: {duration}
🤖 Automatisch erstellt via Zoom Transkript"""


def parse_vtt(vtt_content: str) -> str:
    """
    Parse a WebVTT file and return clean plain text.

    VTT format:
        WEBVTT

        00:00:01.000 --> 00:00:04.000
        Kevin: Hallo, schön dass Sie Zeit haben.

        00:00:04.500 --> 00:00:08.000
        Lead: Ja, danke für die Einladung.

    Returns plain text with one line per cue, duplicate lines collapsed.
    """
    lines = vtt_content.splitlines()
    text_lines: list[str] = []
    last_line = ""

    # Skip header and timestamp lines, collect text cues
    timestamp_pattern = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}")

    skip_next = False
    for line in lines:
        line = line.strip()

        # Skip VTT header
        if line == "WEBVTT" or line.startswith("NOTE") or line.startswith("STYLE"):
            continue

        # Skip timestamp lines — the text line follows
        if timestamp_pattern.match(line):
            skip_next = False
            continue

        # Skip empty lines and cue identifiers (pure numbers)
        if not line or line.isdigit():
            continue

        # Strip HTML tags Zoom sometimes adds (e.g. <v Kevin>text</v>)
        line = re.sub(r"</?v[^>]*>", "", line)
        line = re.sub(r"<[^>]+>", "", line).strip()

        if not line:
            continue

        # Collapse consecutive duplicate lines (Zoom sometimes repeats cues)
        if line == last_line:
            continue

        text_lines.append(line)
        last_line = line

    transcript = "\n".join(text_lines)
    logger.info("VTT parsed: %d lines → %d chars", len(text_lines), len(transcript))
    return transcript


async def summarize_transcript(transcript: str, duration_minutes: int = 0) -> str:
    """
    Summarize a call transcript using Claude Haiku.
    Returns formatted summary text ready for HubSpot Note.
    """
    if not ANTHROPIC_API_KEY:
        raise EnvironmentError("ANTHROPIC_API_KEY not set — cannot summarize transcript")

    if not transcript.strip():
        return "⚠️ Transkript leer — kein Inhalt zum Zusammenfassen."

    duration_str = f"{duration_minutes} Min." if duration_minutes else "unbekannt"

    prompt = _SUMMARY_USER_TEMPLATE.format(
        transcript=transcript[:8000],  # Haiku context limit safety
        duration=duration_str,
    )

    logger.info("Haiku: summarizing transcript (%d chars)", len(transcript))

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 600,
                "system": _SUMMARY_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        r.raise_for_status()
        data = r.json()

    summary = data["content"][0]["text"].strip()
    logger.info("Haiku: summary complete (%d chars)", len(summary))
    return summary


async def process_zoom_vtt(
    vtt_content: str,
    duration_minutes: int = 0,
) -> str:
    """
    Full pipeline: VTT string → parse → summarize.
    Returns formatted HubSpot Note text.
    """
    transcript = parse_vtt(vtt_content)

    if not transcript.strip():
        return (
            "⚠️ VTT-Transkript konnte nicht geparst werden.\n"
            "Bitte Gesprächsnotizen manuell hinzufügen."
        )

    try:
        return await summarize_transcript(transcript, duration_minutes)
    except Exception as e:
        logger.error("call_summarizer: summarization failed: %s", e)
        short = transcript[:1000] + "..." if len(transcript) > 1000 else transcript
        return (
            f"⚠️ AI-Zusammenfassung fehlgeschlagen: {e}\n\n"
            f"Transkript (gekürzt):\n{short}"
        )
