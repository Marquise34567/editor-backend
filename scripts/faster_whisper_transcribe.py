#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path


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

    try:
        model = WhisperModel(args.model, device=device, compute_type=compute_type)
        segments, info = model.transcribe(
            str(input_path),
            language=(str(args.language).strip() or None),
            vad_filter=bool(args.vad_filter),
            beam_size=beam_size,
            word_timestamps=True,
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
        cues.append(
            {
                "text": text,
                "start": round(start, 3),
                "end": round(end, 3),
                "confidence": round(float(avg_logprob), 4) if avg_logprob is not None else None,
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
    }
    sys.stdout.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
