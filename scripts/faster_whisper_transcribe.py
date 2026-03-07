#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from pathlib import Path

FILLER_WORDS = {
    "um",
    "uh",
    "like",
    "basically",
    "literally",
    "actually",
    "honestly",
    "seriously",
}
EMPHASIS_WORDS = {
    "crazy",
    "insane",
    "wild",
    "shocking",
    "secret",
    "proof",
    "never",
    "always",
    "must",
    "now",
    "stop",
    "wait",
    "watch",
    "listen",
    "important",
    "viral",
    "breaking",
    "unbelievable",
    "cannot",
    "can't",
    "cant",
    "no",
    "way",
    "how",
    "why",
    "what",
}
EMOJI_RULES = [
    (re.compile(r"(crazy|insane|wild|shocking|wtf|no\s*way)", re.IGNORECASE), "🤯"),
    (re.compile(r"(fire|hot|viral|legend|win|clutch|craziest|hype)", re.IGNORECASE), "🔥"),
    (re.compile(r"(laugh|funny|lol|lmao|joke)", re.IGNORECASE), "😂"),
    (re.compile(r"(money|cash|million|deal|profit|sales)", re.IGNORECASE), "💸"),
    (re.compile(r"(love|heart|cute)", re.IGNORECASE), "❤️"),
    (re.compile(r"(watch|look|wait|listen|secret|proof)", re.IGNORECASE), "👀"),
]


def _clean_surface(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_token(text: str) -> str:
    # Keep apostrophes so contractions still map (can't, don't, etc.).
    compact = _clean_surface(text).lower()
    return re.sub(r"(^[^a-z0-9']+|[^a-z0-9']+$)", "", compact)


def _is_filler_token(token: str, prev_token: str, next_token: str) -> bool:
    if token in FILLER_WORDS:
        return True
    if token in {"you", "know"} and (prev_token == "you" or next_token == "know"):
        return True
    if token in {"kind", "sort"} and next_token == "of":
        return True
    if token == "of" and prev_token in {"kind", "sort"}:
        return True
    if token in {"i", "mean"} and (prev_token == "i" or next_token == "mean"):
        return True
    return False


def _is_emphasis_token(token: str, surface: str) -> bool:
    if not token:
        return False
    if token in EMPHASIS_WORDS:
        return True
    if any(ch.isdigit() for ch in token):
        return True
    if surface.isupper() and len(token) >= 3:
        return True
    if len(token) >= 8 and token.endswith(("est", "ever", "ing")):
        return True
    return False


def _infer_emoji(surface: str, token: str) -> str:
    sample = f"{surface} {token}".strip()
    for pattern, emoji in EMOJI_RULES:
        if pattern.search(sample):
            return emoji
    return ""


def _normalize_word_rows(segment_words, segment_start: float, segment_end: float):
    rows = []
    if not segment_words:
        return rows
    for raw_word in segment_words:
        surface = _clean_surface(getattr(raw_word, "word", "") or getattr(raw_word, "text", ""))
        if not surface:
            continue
        start = float(getattr(raw_word, "start", segment_start) or segment_start)
        end = float(getattr(raw_word, "end", segment_start) or segment_start)
        if end <= start + 0.005:
            end = start + 0.05
        start = max(segment_start, start)
        end = min(segment_end, end)
        if end <= start + 0.005:
            continue
        probability = getattr(raw_word, "probability", None)
        rows.append(
            {
                "surface": surface,
                "token": _normalize_token(surface),
                "start": round(start, 3),
                "end": round(end, 3),
                "confidence": round(float(probability), 4) if probability is not None else None,
            }
        )
    return rows


def _annotate_words(rows):
    if not rows:
        return []
    annotated = []
    for idx, row in enumerate(rows):
        token = row["token"]
        prev_token = rows[idx - 1]["token"] if idx > 0 else ""
        next_token = rows[idx + 1]["token"] if idx + 1 < len(rows) else ""
        is_filler = _is_filler_token(token, prev_token, next_token)
        emphasis = _is_emphasis_token(token, row["surface"])
        emoji = _infer_emoji(row["surface"], token) if emphasis else ""
        annotated.append(
            {
                "text": row["surface"],
                "start": row["start"],
                "end": row["end"],
                "confidence": row["confidence"],
                "emphasis": bool(emphasis),
                "isFiller": bool(is_filler),
                "emoji": emoji or None,
                "speaker": None,
            }
        )
    return annotated


def _format_srt_timestamp(seconds: float) -> str:
    safe = max(0.0, float(seconds or 0.0))
    millis = int(round(safe * 1000.0))
    hours = millis // 3600000
    millis -= hours * 3600000
    minutes = millis // 60000
    millis -= minutes * 60000
    secs = millis // 1000
    millis -= secs * 1000
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def _write_srt(cues, output_path: Path) -> None:
    lines = []
    idx = 1
    for cue in cues:
        text = str(cue.get("text", "")).strip()
        start = float(cue.get("start", 0.0))
        end = float(cue.get("end", 0.0))
        if not text:
            continue
        if end <= start + 0.01:
            continue
        lines.append(str(idx))
        lines.append(f"{_format_srt_timestamp(start)} --> {_format_srt_timestamp(end)}")
        lines.append(text)
        lines.append("")
        idx += 1
    output_path.write_text("\n".join(lines), encoding="utf-8")


def _resolve_device(requested: str) -> str:
    if requested:
        return requested
    try:
        import torch  # type: ignore

        if torch.cuda.is_available():
            return "cuda"
    except Exception:
        pass
    return "cpu"


def _resolve_compute_type(device: str, requested: str) -> str:
    if requested:
        return requested
    return "float16" if device == "cuda" else "int8"


def main() -> int:
    parser = argparse.ArgumentParser(description="Transcribe media using faster-whisper and emit SRT + JSON.")
    parser.add_argument("--input", required=True, help="Input media path.")
    parser.add_argument("--output-dir", required=True, help="Directory where transcript files are written.")
    parser.add_argument("--base-name", default="", help="Output basename (defaults to input filename stem).")
    parser.add_argument("--model", default="medium", help="faster-whisper model size (e.g. small, medium, large-v3).")
    parser.add_argument("--language", default="", help="Language code, e.g. en.")
    parser.add_argument("--device", default="", help="Device override: cpu or cuda.")
    parser.add_argument("--compute-type", default="", help="Compute type override (int8, float16, etc.).")
    parser.add_argument("--beam-size", default=5, type=int, help="Beam size for decoding.")
    parser.add_argument("--vad-filter", action="store_true", help="Enable VAD filter.")
    parser.add_argument(
        "--no-word-timestamps",
        action="store_true",
        help="Disable word-level timestamps for faster, lower-memory transcription.",
    )
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        sys.stderr.write(f"Input file not found: {input_path}\n")
        return 2

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    base_name = str(args.base_name).strip() or input_path.stem

    try:
        from faster_whisper import WhisperModel  # type: ignore
    except Exception as exc:
        sys.stderr.write(f"Failed to import faster_whisper: {exc}\n")
        return 3

    device = _resolve_device(str(args.device or "").strip().lower())
    compute_type = _resolve_compute_type(device, str(args.compute_type or "").strip())
    beam_size = max(1, min(10, int(args.beam_size or 5)))
    word_timestamps = not bool(args.no_word_timestamps)

    try:
        model = WhisperModel(args.model, device=device, compute_type=compute_type)
        segments, info = model.transcribe(
            str(input_path),
            language=(str(args.language).strip() or None),
            vad_filter=bool(args.vad_filter),
            beam_size=beam_size,
            word_timestamps=word_timestamps,
        )
    except Exception as exc:
        sys.stderr.write(f"Failed to transcribe with faster-whisper: {exc}\n")
        return 4

    cues = []
    for segment in segments:
        text = str(getattr(segment, "text", "") or "").strip()
        start = float(getattr(segment, "start", 0.0) or 0.0)
        end = float(getattr(segment, "end", 0.0) or 0.0)
        avg_logprob = getattr(segment, "avg_logprob", None)
        if not text:
            continue
        if end <= start + 0.01:
            continue
        raw_words = _normalize_word_rows(getattr(segment, "words", None), start, end) if word_timestamps else []
        words = _annotate_words(raw_words)
        cues.append(
            {
                "text": text,
                "start": round(start, 3),
                "end": round(end, 3),
                "confidence": round(float(avg_logprob), 4) if avg_logprob is not None else None,
                "words": words,
                "speaker": None,
            }
        )

    srt_path = output_dir / f"{base_name}.srt"
    json_path = output_dir / f"{base_name}.transcript.json"
    _write_srt(cues, srt_path)
    json_path.write_text(
        json.dumps(
            {
                "language": getattr(info, "language", None),
                "duration": getattr(info, "duration", None),
                "model": args.model,
                "device": device,
                "computeType": compute_type,
                "wordLevelTimestamps": word_timestamps,
                "segments": cues,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result = {
        "ok": True,
        "srtPath": str(srt_path),
        "jsonPath": str(json_path),
        "segmentCount": len(cues),
        "device": device,
        "computeType": compute_type,
        "model": args.model,
        "wordLevelTimestamps": word_timestamps,
    }
    sys.stdout.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
