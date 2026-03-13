"""
Call Summarizer — Zoom Recording → Whisper Transcription → Claude Haiku Summary

Flow:
  1. Receive recording bytes (audio file from Zoom)
  2. Transcribe via OpenAI Whisper API
  3. Summarize via Claude Haiku — structured output for HubSpot Note

Env vars required:
    OPENAI_API_KEY     — for Whisper transcription
    ANTHROPIC_API_KEY  — for Claude Haiku summary
"""

import logging
import os
import tempfile
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Max recording size to process (50 MB — Whisper limit is 25 MB, we check here first)
MAX_RECORDING_BYTES = 50 * 1024 * 1024

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
🤖 Automatisch erstellt via Zoom AI"""


async def transcribe_audio(audio_bytes: bytes, file_extension: str = "m4a") -> str:
    """
    Transcribe audio bytes using OpenAI Whisper API.
    Returns the transcribed text.
    """
    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY not set — cannot transcribe audio")

    if len(audio_bytes) > MAX_RECORDING_BYTES:
        raise ValueError(
            f"Recording too large: {len(audio_bytes) / 1024 / 1024:.1f} MB "
            f"(max {MAX_RECORDING_BYTES / 1024 / 1024:.0f} MB)"
        )

    logger.info("Whisper: transcribing %.1f MB audio file", len(audio_bytes) / 1024 / 1024)

    # Write to temp file — Whisper API requires a file upload
    with tempfile.NamedTemporaryFile(suffix=f".{file_extension}", delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            with open(tmp_path, "rb") as f:
                r = await client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    files={"file": (f"recording.{file_extension}", f, "audio/mpeg")},
                    data={
                        "model": "whisper-1",
                        "language": "de",   # German — change if multilingual needed
                        "response_format": "text",
                    },
                )
                r.raise_for_status()
                transcript = r.text.strip()

        logger.info("Whisper: transcription complete (%d chars)", len(transcript))
        return transcript

    finally:
        import os as _os
        _os.unlink(tmp_path)


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


async def process_zoom_recording(
    audio_bytes: bytes,
    file_extension: str = "m4a",
    duration_minutes: int = 0,
) -> str:
    """
    Full pipeline: audio bytes → transcription → summary.
    Returns formatted HubSpot Note text.
    """
    try:
        transcript = await transcribe_audio(audio_bytes, file_extension)
    except Exception as e:
        logger.error("call_summarizer: transcription failed: %s", e)
        return f"⚠️ Transkription fehlgeschlagen: {e}\n\nBitte manuell zusammenfassen."

    try:
        summary = await summarize_transcript(transcript, duration_minutes)
    except Exception as e:
        logger.error("call_summarizer: summarization failed: %s", e)
        # Return raw transcript as fallback
        short_transcript = transcript[:1000] + "..." if len(transcript) > 1000 else transcript
        return (
            f"⚠️ AI-Zusammenfassung fehlgeschlagen: {e}\n\n"
            f"Transkript (gekürzt):\n{short_transcript}"
        )

    return summary
