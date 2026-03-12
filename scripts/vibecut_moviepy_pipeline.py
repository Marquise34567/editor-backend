#!/usr/bin/env python3
"""
MoviePy helper pipeline for VibeCut.
- Vertical mode: outputs up to 3 highlight clips (9:16) and a combined file.
- Horizontal mode: outputs a single 16:9 combined file.
- Vertical mode includes a shorts caption agent with word-by-word highlighting.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from PIL import Image, ImageDraw, ImageFont


EMOJI_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(crazy|insane|wild|shocking|wtf|no\s*way)", re.IGNORECASE), "🤯"),
    (re.compile(r"(fire|hot|viral|legend|win|clutch|hype)", re.IGNORECASE), "🔥"),
    (re.compile(r"(laugh|funny|lol|lmao|joke)", re.IGNORECASE), "😂"),
    (re.compile(r"(money|cash|million|deal|profit|sales)", re.IGNORECASE), "💸"),
    (re.compile(r"(watch|look|wait|listen|secret|proof)", re.IGNORECASE), "👀"),
]

KEYWORD_TOKENS = {
    "crazy",
    "insane",
    "wild",
    "shocking",
    "viral",
    "secret",
    "proof",
    "watch",
    "listen",
    "stop",
    "wait",
    "now",
    "never",
    "always",
    "must",
    "win",
    "money",
    "million",
}

FONT_PATH_CANDIDATES = [
    os.environ.get("VIBECUT_CAPTION_FONT_PATH", "").strip(),
    r"C:\Windows\Fonts\Montserrat-Bold.ttf",
    r"C:\Windows\Fonts\montserrat-bold.ttf",
    r"C:\Windows\Fonts\arialbd.ttf",
    r"C:\Windows\Fonts\seguisb.ttf",
    "/usr/share/fonts/truetype/montserrat/Montserrat-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]

EMOJI_FONT_CANDIDATES = [
    os.environ.get("VIBECUT_EMOJI_FONT_PATH", "").strip(),
    r"C:\Windows\Fonts\seguiemj.ttf",
    r"C:\Windows\Fonts\SegoeUIEmoji.ttf",
    "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
]


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
    from moviepy.editor import ColorClip, CompositeVideoClip

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
    from moviepy.editor import ColorClip, CompositeVideoClip

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


def _resolve_existing_path(candidates: Iterable[str]) -> str | None:
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.exists():
            return str(path)
    return None


def _load_font(font_path: str | None, size: int):
    safe_size = max(12, int(size))
    if font_path:
        try:
            return ImageFont.truetype(font_path, safe_size)
        except Exception:
            pass
    try:
        fallback = _resolve_existing_path(FONT_PATH_CANDIDATES[1:])
        if fallback:
            return ImageFont.truetype(fallback, safe_size)
    except Exception:
        pass
    return ImageFont.load_default()


def _tokenize(text: str) -> str:
    compact = str(text or "").strip().lower()
    return re.sub(r"(^[^a-z0-9']+|[^a-z0-9']+$)", "", compact)


def _infer_emoji(text: str) -> str | None:
    sample = str(text or "")
    for pattern, emoji in EMOJI_RULES:
        if pattern.search(sample):
            return emoji
    return None


def _parse_bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return fallback


def _normalize_word(text: str, start: float, end: float, confidence: Any = None, emphasis: bool = False, emoji: str | None = None) -> dict[str, Any] | None:
    token = str(text or "").strip()
    if not token:
        return None
    safe_start = max(0.0, float(start or 0.0))
    safe_end = max(safe_start + 0.04, float(end or 0.0))
    confidence_num: float | None = None
    try:
        confidence_num = float(confidence) if confidence is not None else None
    except Exception:
        confidence_num = None
    token_norm = _tokenize(token)
    emphasis_flag = bool(emphasis) or (token_norm in KEYWORD_TOKENS)
    emoji_value = str(emoji).strip() if emoji else (_infer_emoji(token) if emphasis_flag else None)
    return {
        "text": token,
        "start": round(safe_start, 3),
        "end": round(safe_end, 3),
        "confidence": confidence_num,
        "emphasis": emphasis_flag,
        "emoji": emoji_value or None,
    }


def _synthesize_words_from_segment(segment: dict[str, Any]) -> list[dict[str, Any]]:
    text = str(segment.get("text", "")).strip()
    if not text:
        return []
    start = float(segment.get("start", 0.0) or 0.0)
    end = float(segment.get("end", start + 0.2) or (start + 0.2))
    if end <= start:
        end = start + 0.2
    tokens = [part for part in re.split(r"\s+", text) if part]
    if not tokens:
        return []
    duration = max(0.2, end - start)
    step = duration / max(1, len(tokens))
    words: list[dict[str, Any]] = []
    for index, token in enumerate(tokens):
        ws = start + index * step
        we = start + (index + 1) * step
        normalized = _normalize_word(token, ws, we)
        if normalized:
            words.append(normalized)
    return words


def _extract_words_from_transcript_payload(payload: Any, require_word_timestamps: bool) -> list[dict[str, Any]]:
    if payload is None:
        return []

    words: list[dict[str, Any]] = []
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_word(
                text=item.get("text") or item.get("word") or "",
                start=item.get("start"),
                end=item.get("end"),
                confidence=item.get("confidence"),
                emphasis=_parse_bool(item.get("emphasis"), False),
                emoji=item.get("emoji"),
            )
            if normalized:
                words.append(normalized)
    elif isinstance(payload, dict):
        segments = payload.get("segments")
        if isinstance(segments, list):
            for segment in segments:
                if not isinstance(segment, dict):
                    continue
                segment_words = segment.get("words")
                if isinstance(segment_words, list) and segment_words:
                    for item in segment_words:
                        if not isinstance(item, dict):
                            continue
                        normalized = _normalize_word(
                            text=item.get("text") or item.get("word") or "",
                            start=item.get("start", segment.get("start")),
                            end=item.get("end", segment.get("end")),
                            confidence=item.get("confidence"),
                            emphasis=_parse_bool(item.get("emphasis"), False),
                            emoji=item.get("emoji"),
                        )
                        if normalized:
                            words.append(normalized)
                elif not require_word_timestamps:
                    words.extend(_synthesize_words_from_segment(segment))
        elif not require_word_timestamps:
            words.extend(_synthesize_words_from_segment(payload))

    words = [w for w in words if float(w["end"]) > float(w["start"]) + 0.01]
    words.sort(key=lambda row: (float(row["start"]), float(row["end"])))
    return words


def _extract_word_timestamps_with_whisper(video_path: str) -> list[dict[str, Any]]:
    try:
        from faster_whisper import WhisperModel  # type: ignore
    except Exception:
        return []

    device = "cpu"
    compute_type = "int8"
    try:
        import torch  # type: ignore

        if torch.cuda.is_available():
            device = "cuda"
            compute_type = "float16"
    except Exception:
        pass

    model_name = str(os.environ.get("VIBECUT_WHISPER_MODEL", "small")).strip() or "small"
    try:
        model = WhisperModel(model_name, device=device, compute_type=compute_type)
        segments, _ = model.transcribe(
            video_path,
            vad_filter=True,
            beam_size=5,
            word_timestamps=True,
        )
    except Exception:
        return []

    words: list[dict[str, Any]] = []
    for segment in segments:
        segment_start = float(getattr(segment, "start", 0.0) or 0.0)
        segment_end = float(getattr(segment, "end", segment_start + 0.2) or (segment_start + 0.2))
        segment_words = getattr(segment, "words", None) or []
        for item in segment_words:
            text = str(getattr(item, "word", "") or getattr(item, "text", "")).strip()
            start = float(getattr(item, "start", segment_start) or segment_start)
            end = float(getattr(item, "end", start + 0.05) or (start + 0.05))
            start = max(segment_start, start)
            end = min(segment_end, max(end, start + 0.04))
            normalized = _normalize_word(text, start, end, confidence=getattr(item, "probability", None))
            if normalized:
                words.append(normalized)

    words.sort(key=lambda row: (float(row["start"]), float(row["end"])))
    return words


def _load_transcript_payload(raw: str) -> Any:
    if not raw:
        return {}

    candidate_path = Path(raw).expanduser()
    if candidate_path.exists() and candidate_path.is_file():
        try:
            return json.loads(candidate_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    try:
        return json.loads(raw)
    except Exception:
        return {}


def _measure_text(text: str, font, stroke_width: int = 0) -> tuple[int, int]:
    probe = Image.new("RGBA", (8, 8), (0, 0, 0, 0))
    draw = ImageDraw.Draw(probe)
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font, stroke_width=max(0, stroke_width))
    return max(1, right - left), max(1, bottom - top)


def _render_text_rgba(
    text: str,
    font,
    fill: tuple[int, int, int, int],
    stroke_fill: tuple[int, int, int, int] | None = None,
    stroke_width: int = 0,
    padding: int = 6,
) -> np.ndarray:
    probe = Image.new("RGBA", (8, 8), (0, 0, 0, 0))
    draw = ImageDraw.Draw(probe)
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font, stroke_width=max(0, stroke_width))
    text_w = max(1, right - left)
    text_h = max(1, bottom - top)
    pad = max(0, int(padding))
    canvas = Image.new("RGBA", (text_w + pad * 2, text_h + pad * 2), (0, 0, 0, 0))
    painter = ImageDraw.Draw(canvas)
    painter.text(
        (pad - left, pad - top),
        text,
        font=font,
        fill=fill,
        stroke_width=max(0, stroke_width),
        stroke_fill=stroke_fill,
    )
    return np.array(canvas)


def _rounded_box_rgba(width: int, height: int, fill: tuple[int, int, int, int], radius: int) -> np.ndarray:
    safe_w = max(2, int(width))
    safe_h = max(2, int(height))
    safe_radius = max(0, min(int(radius), min(safe_w, safe_h) // 2))
    canvas = Image.new("RGBA", (safe_w, safe_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)
    draw.rounded_rectangle((0, 0, safe_w - 1, safe_h - 1), radius=safe_radius, fill=fill)
    return np.array(canvas)


def _chunk_words(words: list[dict[str, Any]], max_words: int = 3, max_chars: int = 15) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    char_count = 0

    for word in words:
        token = str(word.get("text", "")).strip()
        if not token:
            continue
        token_len = len(token)
        projected = char_count + (1 if current else 0) + token_len
        if current and (len(current) >= max_words or projected > max_chars):
            chunks.append(
                {
                    "words": current,
                    "start": float(current[0]["start"]),
                    "end": float(current[-1]["end"]),
                    "text": " ".join(str(item["text"]) for item in current),
                }
            )
            current = []
            char_count = 0
        current.append(word)
        char_count += (1 if len(current) > 1 else 0) + token_len

    if current:
        chunks.append(
            {
                "words": current,
                "start": float(current[0]["start"]),
                "end": float(current[-1]["end"]),
                "text": " ".join(str(item["text"]) for item in current),
            }
        )
    return chunks


def _resolve_safe_caption_y(height: int) -> int:
    if height >= 1800:
        return 1400
    desired = int(round(height * 0.73))
    return int(clamp(desired, int(height * 0.58), int(height * 0.8)))


def _pop_scale(local_t: float) -> float:
    t = float(local_t or 0.0)
    if t <= 0.0:
        return 0.9
    rise = 0.08
    settle = 0.08
    if t < rise:
        return 0.9 + 0.2 * (t / rise)
    if t < rise + settle:
        return 1.1 - 0.1 * ((t - rise) / settle)
    return 1.0


def _animate_with_pop(clip, x: float, y: float):
    base_w = float(clip.w)
    base_h = float(clip.h)

    def _scale_fn(t: float) -> float:
        return _pop_scale(t)

    def _position_fn(t: float) -> tuple[float, float]:
        scale = _scale_fn(t)
        dx = (base_w - base_w * scale) / 2.0
        dy = (base_h - base_h * scale) / 2.0
        return (x + dx, y + dy)

    return clip.resize(_scale_fn).set_position(_position_fn)


def _build_caption_layers_for_clip(clip, words: list[dict[str, Any]]):
    from moviepy.editor import ImageClip

    if not words:
        return []

    clip_duration = float(clip.duration or 0.0)
    if clip_duration <= 0.01:
        return []

    trimmed_words = []
    for row in words:
        ws = clamp(float(row.get("start", 0.0) or 0.0), 0.0, clip_duration)
        we = clamp(float(row.get("end", ws + 0.04) or (ws + 0.04)), 0.0, clip_duration)
        if we <= ws + 0.01:
            continue
        trimmed_words.append(
            {
                **row,
                "start": round(ws, 3),
                "end": round(max(ws + 0.04, we), 3),
            }
        )
    if not trimmed_words:
        return []

    font_path = _resolve_existing_path(FONT_PATH_CANDIDATES)
    emoji_font_path = _resolve_existing_path(EMOJI_FONT_CANDIDATES) or font_path
    font_size = int(clamp(round(float(clip.w) * 0.082), 56, 106))
    emoji_size = int(clamp(round(font_size * 0.66), 34, 92))
    text_font = _load_font(font_path, font_size)
    emoji_font = _load_font(emoji_font_path, emoji_size)
    stroke = max(2, int(round(font_size * 0.06)))
    text_pad = max(4, int(round(font_size * 0.08)))

    safe_y = _resolve_safe_caption_y(int(clip.h))
    space_w, _ = _measure_text(" ", text_font, stroke_width=stroke)

    chunks = _chunk_words(trimmed_words, max_words=3, max_chars=15)
    if not chunks:
        return []

    layers = []
    for chunk in chunks:
        chunk_start = clamp(float(chunk["start"]), 0.0, clip_duration)
        chunk_end = clamp(float(chunk["end"]), 0.0, clip_duration)
        if chunk_end <= chunk_start + 0.01:
            continue

        word_rows: list[dict[str, Any]] = []
        line_width = 0
        line_height = 0
        for index, word in enumerate(chunk["words"]):
            token = str(word.get("text", "")).strip()
            if not token:
                continue
            text_w, text_h = _measure_text(token, text_font, stroke_width=stroke)
            word_rows.append(
                {
                    **word,
                    "text": token,
                    "text_w": text_w,
                    "text_h": text_h,
                    "x": line_width,
                }
            )
            line_width += text_w
            if index < len(chunk["words"]) - 1:
                line_width += space_w
            line_height = max(line_height, text_h)

        if not word_rows or line_width <= 0 or line_height <= 0:
            continue

        line_x = (float(clip.w) - float(line_width)) / 2.0
        line_top = float(safe_y) - float(line_height) / 2.0

        for row in word_rows:
            word_start = clamp(float(row["start"]), chunk_start, chunk_end)
            word_end = clamp(float(row["end"]), chunk_start, chunk_end)
            if word_end <= word_start + 0.01:
                continue

            x_word = line_x + float(row["x"])
            y_word = line_top + (line_height - float(row["text_h"])) / 2.0

            base_text_img = _render_text_rgba(
                row["text"],
                text_font,
                fill=(255, 255, 255, 255),
                stroke_fill=(0, 0, 0, 235),
                stroke_width=stroke,
                padding=text_pad,
            )
            base_text_clip = (
                ImageClip(base_text_img)
                .set_start(chunk_start)
                .set_end(chunk_end)
                .set_position((x_word - text_pad, y_word - text_pad))
            )
            layers.append(base_text_clip)

            highlight_box_width = int(
                max(
                    row["text_w"] + 40,
                    row["text_w"] + round(font_size * 0.5),
                )
            )
            highlight_box_height = int(row["text_h"] + 20)
            highlight_pad_x = (highlight_box_width - int(row["text_w"])) / 2.0
            highlight_pad_y = (highlight_box_height - int(row["text_h"])) / 2.0
            is_emphasis = bool(row.get("emphasis"))
            box_color = (255, 204, 0, 246) if is_emphasis else (0, 0, 0, 204)
            active_text_color = (20, 20, 20, 255) if is_emphasis else (255, 255, 255, 255)
            active_stroke = (0, 0, 0, 0) if is_emphasis else (0, 0, 0, 220)

            box_img = _rounded_box_rgba(
                highlight_box_width,
                highlight_box_height,
                fill=box_color,
                radius=int(clamp(round(highlight_box_height * 0.24), 10, 26)),
            )
            box_x = x_word - highlight_pad_x
            box_y = y_word - highlight_pad_y
            box_clip = (
                ImageClip(box_img)
                .set_start(word_start)
                .set_end(word_end)
            )
            layers.append(_animate_with_pop(box_clip, box_x, box_y))

            active_text_img = _render_text_rgba(
                row["text"],
                text_font,
                fill=active_text_color,
                stroke_fill=active_stroke,
                stroke_width=0 if is_emphasis else max(1, stroke - 1),
                padding=text_pad,
            )
            active_text_clip = (
                ImageClip(active_text_img)
                .set_start(word_start)
                .set_end(word_end)
            )
            layers.append(_animate_with_pop(active_text_clip, x_word - text_pad, y_word - text_pad))

            popup_emoji = str(row.get("emoji") or "").strip()
            if popup_emoji:
                emoji_img = _render_text_rgba(
                    popup_emoji,
                    emoji_font,
                    fill=(255, 255, 255, 255),
                    stroke_fill=(0, 0, 0, 210),
                    stroke_width=max(0, int(round(emoji_size * 0.08))),
                    padding=max(3, int(round(emoji_size * 0.14))),
                )
                popup_duration = clamp(min(0.6, (word_end - word_start) + 0.18), 0.24, 0.6)
                popup_end = clamp(word_start + popup_duration, word_start + 0.12, clip_duration)
                hash_shift = (sum(ord(ch) for ch in row["text"]) % 80) - 40
                emoji_x = clamp(x_word + row["text_w"] / 2.0 - emoji_img.shape[1] / 2.0 + hash_shift, 20, clip.w - emoji_img.shape[1] - 20)
                emoji_y = max(40, line_top - emoji_img.shape[0] - int(round(font_size * 0.45)))
                emoji_clip = (
                    ImageClip(emoji_img)
                    .set_start(word_start)
                    .set_end(popup_end)
                )
                layers.append(_animate_with_pop(emoji_clip, emoji_x, emoji_y))

    return layers


def _slice_words_for_segment(
    words: list[dict[str, Any]],
    segment_start: float,
    segment_end: float,
    speed: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    safe_speed = max(1.0, float(speed or 1.0))
    src_start = float(segment_start or 0.0)
    src_end = float(segment_end or src_start)
    if src_end <= src_start:
        return out

    for row in words:
        ws = float(row.get("start", 0.0) or 0.0)
        we = float(row.get("end", ws + 0.04) or (ws + 0.04))
        if we <= src_start or ws >= src_end:
            continue
        clipped_start = max(src_start, ws)
        clipped_end = min(src_end, we)
        if clipped_end <= clipped_start + 0.01:
            continue
        local_start = (clipped_start - src_start) / safe_speed
        local_end = (clipped_end - src_start) / safe_speed
        normalized = {
            **row,
            "start": round(max(0.0, local_start), 3),
            "end": round(max(local_start + 0.04, local_end), 3),
        }
        out.append(normalized)
    out.sort(key=lambda item: (float(item["start"]), float(item["end"])))
    return out


def _apply_caption_agent_to_clip(clip, words: list[dict[str, Any]]):
    from moviepy.editor import CompositeVideoClip

    layers = _build_caption_layers_for_clip(clip, words)
    composite = CompositeVideoClip([clip, *layers], size=(clip.w, clip.h))
    if clip.audio is not None:
        composite = composite.set_audio(clip.audio)
    return composite


def generate_caption_agent(video_path: str, transcript: Any):
    """
    Build a full caption-agent composite from an input video and transcript payload.
    Returns a CompositeVideoClip with animated word-by-word layers merged.
    """
    from moviepy.editor import CompositeVideoClip, VideoFileClip

    base = VideoFileClip(str(video_path))
    words = _extract_words_from_transcript_payload(transcript, require_word_timestamps=True)
    if not words:
        words = _extract_word_timestamps_with_whisper(str(video_path))
    if not words:
        words = _extract_words_from_transcript_payload(transcript, require_word_timestamps=False)
    layers = _build_caption_layers_for_clip(base, words)
    composite = CompositeVideoClip([base, *layers], size=(base.w, base.h))
    if base.audio is not None:
        composite = composite.set_audio(base.audio)
    return composite


def main() -> int:
    parser = argparse.ArgumentParser(description="VibeCut MoviePy pipeline")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--mode", default="vertical", choices=["vertical", "horizontal"])
    parser.add_argument("--segments-json", default="[]")
    parser.add_argument("--transcript-json", default="")
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
    transcript_payload = _load_transcript_payload(str(args.transcript_json or "").strip())

    full_words: list[dict[str, Any]] = []
    transcript_source = "none"
    if args.mode == "vertical":
        full_words = _extract_words_from_transcript_payload(transcript_payload, require_word_timestamps=True)
        if full_words:
            transcript_source = "transcript_words"
        if not full_words:
            full_words = _extract_word_timestamps_with_whisper(str(input_path))
            if full_words:
                transcript_source = "whisper"
        if not full_words:
            full_words = _extract_words_from_transcript_payload(transcript_payload, require_word_timestamps=False)
            if full_words:
                transcript_source = "transcript_synth"

    try:
        with VideoFileClip(str(input_path)) as source:
            duration = float(source.duration or 0.0)
            if duration <= 0.0:
                print(json.dumps({"ok": False, "error": "invalid_duration", "clipPaths": []}))
                return 0

            segments = parse_segments(args.segments_json, duration)
            if not segments:
                segments = [{"start": 0.0, "end": round(min(duration, 20.0), 3), "speed": 1.0}]

            if args.mode == "vertical":
                segments = segments[:3]

            for idx, segment in enumerate(segments, start=1):
                start = float(segment["start"])
                end = float(segment["end"])
                speed = float(segment.get("speed", 1.0) or 1.0)
                sub = None
                processed = None
                try:
                    sub = source.subclip(start, end)
                    if speed > 1.001:
                        sub = sub.fx(vfx.speedx, factor=speed)

                    processed = fit_clip_vertical(sub) if args.mode == "vertical" else fit_clip_horizontal(sub)

                    if args.mode == "vertical" and full_words:
                        segment_words = _slice_words_for_segment(full_words, start, end, speed)
                        if segment_words:
                            processed = _apply_caption_agent_to_clip(processed, segment_words)

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
                finally:
                    if processed is not None:
                        try:
                            processed.close()
                        except Exception:
                            pass
                    if sub is not None:
                        try:
                            sub.close()
                        except Exception:
                            pass

            if clip_paths:
                combined_inputs = [VideoFileClip(path) for path in clip_paths]
                combined = None
                try:
                    combined = concatenate_videoclips(combined_inputs, method="compose")
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
                finally:
                    if combined is not None:
                        try:
                            combined.close()
                        except Exception:
                            pass
                    for clip in combined_inputs:
                        try:
                            clip.close()
                        except Exception:
                            pass
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"pipeline_failed:{exc}", "clipPaths": []}))
        return 0

    print(
        json.dumps(
            {
                "ok": True,
                "clipPaths": clip_paths,
                "combinedPath": str(combined_path),
                "wordTimestampSource": transcript_source,
                "wordTimestampCount": len(full_words),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
