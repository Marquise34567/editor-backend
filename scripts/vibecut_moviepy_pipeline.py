#!/usr/bin/env python3
"""
MoviePy helper pipeline for VibeCut.
- Vertical mode: outputs up to 3 highlight clips (9:16) and a combined file.
- Horizontal mode: outputs a single 16:9 combined file.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def parse_segments(raw: str, duration: float) -> list[dict[str, float]]:
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = []
    out: list[dict[str, float]] = []
    if not isinstance(parsed, list):
        return out
    for item in parsed:
        if not isinstance(item, dict):
            continue
        start = clamp(float(item.get("start", 0.0) or 0.0), 0.0, duration)
        end = clamp(float(item.get("end", 0.0) or 0.0), 0.0, duration)
        speed = clamp(float(item.get("speed", 1.0) or 1.0), 1.0, 1.8)
        if end - start < 0.35:
            continue
        out.append({"start": round(start, 3), "end": round(end, 3), "speed": round(speed, 3)})
    out.sort(key=lambda row: row["start"])
    return out


def fit_clip_vertical(clip):
    from moviepy.editor import CompositeVideoClip, ColorClip

    target_w, target_h = 1080, 1920
    source_aspect = clip.w / clip.h
    target_aspect = target_w / target_h

    if source_aspect > target_aspect:
        fitted = clip.resize(height=target_h)
        x1 = int((fitted.w - target_w) / 2)
        fitted = fitted.crop(x1=x1, y1=0, x2=x1 + target_w, y2=target_h)
    else:
        fitted = clip.resize(width=target_w)
        y1 = int((fitted.h - target_h) / 2)
        fitted = fitted.crop(x1=0, y1=y1, x2=target_w, y2=y1 + target_h)

    # Slight energetic zoom profile for vertical output.
    fitted = fitted.resize(lambda t: 1.02 + 0.035 * min(1.0, t / 0.6))

    bg = ColorClip(size=(target_w, target_h), color=(0, 0, 0), duration=fitted.duration)
    if fitted.audio is not None:
        bg = bg.set_audio(fitted.audio)
    out = CompositeVideoClip([bg, fitted.set_position("center")], size=(target_w, target_h))
    return out


def fit_clip_horizontal(clip):
    from moviepy.editor import CompositeVideoClip, ColorClip

    target_w, target_h = 1920, 1080
    source_aspect = clip.w / clip.h
    target_aspect = target_w / target_h

    if source_aspect > target_aspect:
        fitted = clip.resize(width=target_w)
    else:
        fitted = clip.resize(height=target_h)

    bg = ColorClip(size=(target_w, target_h), color=(0, 0, 0), duration=fitted.duration)
    if fitted.audio is not None:
        bg = bg.set_audio(fitted.audio)

    out = CompositeVideoClip([bg, fitted.set_position("center")], size=(target_w, target_h))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="VibeCut MoviePy pipeline")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--mode", default="vertical", choices=["vertical", "horizontal"])
    parser.add_argument("--segments-json", default="[]")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(json.dumps({"ok": False, "error": "missing_input", "clipPaths": []}))
        return 0

    try:
        from moviepy.editor import VideoFileClip, concatenate_videoclips, vfx
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"moviepy_import_failed:{exc}", "clipPaths": []}))
        return 0

    clip_paths: list[str] = []
    combined_path = output_dir / "combined.mp4"

    try:
        with VideoFileClip(str(input_path)) as source:
            duration = float(source.duration or 0.0)
            if duration <= 0.0:
                print(json.dumps({"ok": False, "error": "invalid_duration", "clipPaths": []}))
                return 0

            segments = parse_segments(args.segments_json, duration)
            if not segments:
                segments = [(0.0, min(duration, 20.0))]

            if args.mode == "vertical":
                segments = segments[:3]

            built_clips = []
            for idx, segment in enumerate(segments, start=1):
                start = float(segment["start"])
                end = float(segment["end"])
                speed = float(segment.get("speed", 1.0) or 1.0)

                sub = source.subclip(start, end)
                if speed > 1.001:
                    sub = sub.fx(vfx.speedx, factor=speed)
                processed = fit_clip_vertical(sub) if args.mode == "vertical" else fit_clip_horizontal(sub)

                out_path = output_dir / f"clip_{idx:02d}.mp4"
                processed.write_videofile(
                    str(out_path),
                    fps=30,
                    codec="libx264",
                    audio_codec="aac",
                    bitrate="8M" if args.mode == "vertical" else "10M",
                    audio_bitrate="128k",
                    ffmpeg_params=["-pix_fmt", "yuv420p", "-movflags", "+faststart"],
                    logger=None,
                )
                clip_paths.append(str(out_path))
                built_clips.append(processed)

            if built_clips:
                combined = concatenate_videoclips(built_clips, method="compose")
                combined.write_videofile(
                    str(combined_path),
                    fps=30,
                    codec="libx264",
                    audio_codec="aac",
                    bitrate="8M" if args.mode == "vertical" else "10M",
                    audio_bitrate="128k",
                    ffmpeg_params=["-pix_fmt", "yuv420p", "-movflags", "+faststart"],
                    logger=None,
                )
                combined.close()

            for clip in built_clips:
                clip.close()
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"pipeline_failed:{exc}", "clipPaths": []}))
        return 0

    print(
        json.dumps(
            {
                "ok": True,
                "clipPaths": clip_paths,
                "combinedPath": str(combined_path),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
