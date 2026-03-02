#!/usr/bin/env python3
"""
Build a retention-first binge edit plan from video + optional transcript JSON.

Outputs:
- Structured analysis JSON with suggested hook, dopamine hits, and ending cliffhanger.
- FFmpeg command list to generate tease clips with text overlays and final concat.

The module is importable for app integration and runnable as a CLI.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


SURPRISE_KEYWORDS = {
    "crazy",
    "insane",
    "wild",
    "wtf",
    "unexpected",
    "unbelievable",
    "shocking",
    "plot twist",
    "never",
    "no way",
    "secret",
    "exposed",
    "busted",
    "boom",
}
INTRIGUE_KEYWORDS = {
    "wait",
    "watch this",
    "what happens",
    "coming up",
    "later",
    "next",
    "but then",
    "you won't believe",
    "here is why",
    "the truth",
    "mystery",
    "question",
    "clue",
}
HUMOR_KEYWORDS = {
    "funny",
    "hilarious",
    "lol",
    "lmao",
    "joke",
    "meme",
    "laugh",
    "roast",
    "comedy",
}
CLIFFHANGER_KEYWORDS = {
    "to be continued",
    "part 2",
    "next episode",
    "next time",
    "unfinished",
    "almost",
    "not yet",
    "stay tuned",
    "then this happened",
    "but wait",
    "what happened next",
}
POSITIVE_LEXICON = {
    "amazing",
    "great",
    "awesome",
    "happy",
    "love",
    "best",
    "win",
    "success",
}
NEGATIVE_LEXICON = {
    "bad",
    "awful",
    "angry",
    "sad",
    "hate",
    "scared",
    "fear",
    "failure",
    "disaster",
}


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9']+", text.lower())


def count_keyword_hits(text: str, keywords: set[str]) -> int:
    compact = text.lower()
    token_set = set(normalize_tokens(compact))
    hits = 0
    for phrase in keywords:
        if " " in phrase:
            if phrase in compact:
                hits += 1
        elif phrase in token_set:
            hits += 1
    return hits


def safe_relpath_label(path: Path) -> str:
    try:
        return str(path)
    except Exception:
        return path.name


def escape_drawtext(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace(":", r"\:")
        .replace("'", r"\'")
        .replace("%", r"\%")
        .replace(",", r"\,")
    )


@dataclass
class TranscriptSegment:
    start: float
    end: float
    text: str
    confidence: float | None = None


@dataclass
class MomentCandidate:
    start: float
    end: float
    text: str
    sentiment_label: str
    sentiment_intensity: float
    surprise_score: float
    intrigue_score: float
    humor_score: float
    cliff_score: float
    audio_energy: float
    hook_score: float
    dopamine_score: float
    ending_score: float

    @property
    def center(self) -> float:
        return (self.start + self.end) / 2.0


@dataclass
class PlannedMoment:
    id: str
    role: str
    source_start: float
    source_end: float
    tease_start: float
    tease_end: float
    rating: float
    reason: str
    overlay_text: str
    transcript_excerpt: str


class SentimentScorer:
    def __init__(self, model_name: str = "distilbert-base-uncased-finetuned-sst-2-english"):
        self.model_name = model_name
        self._pipeline = None
        self._pipeline_attempted = False
        self.backend = "lexical"
        self.error: str | None = None

    def _ensure_pipeline(self) -> None:
        if self._pipeline_attempted:
            return
        self._pipeline_attempted = True
        try:
            from transformers import pipeline  # type: ignore

            self._pipeline = pipeline("sentiment-analysis", model=self.model_name, tokenizer=self.model_name)
            self.backend = "transformers"
        except Exception as exc:
            self._pipeline = None
            self.backend = "lexical"
            self.error = str(exc)

    def _lexical_score(self, text: str) -> tuple[str, float]:
        tokens = normalize_tokens(text)
        if not tokens:
            return ("neutral", 0.0)
        pos = sum(1 for token in tokens if token in POSITIVE_LEXICON)
        neg = sum(1 for token in tokens if token in NEGATIVE_LEXICON)
        total = max(1, len(tokens))
        delta = (pos - neg) / total
        intensity = clamp(abs(delta) * 8.0, 0.0, 1.0)
        if delta > 0.02:
            return ("positive", intensity)
        if delta < -0.02:
            return ("negative", intensity)
        return ("neutral", intensity * 0.6)

    def score_many(self, texts: list[str]) -> list[tuple[str, float]]:
        self._ensure_pipeline()
        if self._pipeline is None or not texts:
            return [self._lexical_score(text) for text in texts]
        try:
            outputs = self._pipeline(texts, truncation=True)
            scored: list[tuple[str, float]] = []
            for output in outputs:
                label_raw = clean_text(output.get("label", "")).lower()
                confidence = clamp(to_float(output.get("score", 0.0), 0.0), 0.0, 1.0)
                if "neg" in label_raw:
                    label = "negative"
                elif "pos" in label_raw:
                    label = "positive"
                else:
                    label = "neutral"
                intensity = abs((confidence - 0.5) * 2.0)
                scored.append((label, clamp(intensity, 0.0, 1.0)))
            return scored
        except Exception:
            return [self._lexical_score(text) for text in texts]


def parse_transcript_segments(transcript_path: Path | None, duration: float) -> list[TranscriptSegment]:
    if transcript_path is None:
        return []
    if not transcript_path.exists():
        return []
    try:
        raw = json.loads(transcript_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    candidate_rows: Any = []
    if isinstance(raw, dict):
        candidate_rows = (
            raw.get("segments")
            or raw.get("transcript")
            or raw.get("captions")
            or raw.get("cues")
            or []
        )
    elif isinstance(raw, list):
        candidate_rows = raw

    segments: list[TranscriptSegment] = []
    if not isinstance(candidate_rows, list):
        return segments

    for row in candidate_rows:
        if not isinstance(row, dict):
            continue
        text = clean_text(
            row.get("text")
            or row.get("caption")
            or row.get("content")
            or row.get("transcript")
            or ""
        )
        if not text:
            continue
        start = to_float(
            row.get("start")
            if row.get("start") is not None
            else row.get("start_time", row.get("begin", 0.0)),
            0.0,
        )
        end = to_float(
            row.get("end")
            if row.get("end") is not None
            else row.get("end_time", row.get("finish", 0.0)),
            0.0,
        )
        if end <= start:
            duration_guess = to_float(row.get("duration"), 0.0)
            if duration_guess > 0:
                end = start + duration_guess
        start = clamp(start, 0.0, duration)
        end = clamp(end, 0.0, duration)
        if end - start < 0.2:
            continue
        confidence_raw = row.get("confidence")
        confidence = to_float(confidence_raw, 0.0) if confidence_raw is not None else None
        segments.append(
            TranscriptSegment(
                start=round(start, 3),
                end=round(end, 3),
                text=text,
                confidence=round(confidence, 4) if confidence is not None else None,
            )
        )
    segments.sort(key=lambda item: item.start)
    return segments


def build_synthetic_segments(duration: float, min_len: float, max_len: float) -> list[TranscriptSegment]:
    if duration <= 0:
        return []
    window = clamp(duration * 0.12, min_len, max_len)
    if duration < window:
        return [TranscriptSegment(start=0.0, end=round(duration, 3), text="")]
    step = max(2.0, window * 0.66)
    out: list[TranscriptSegment] = []
    current = 0.0
    while current < duration:
        start = current
        end = min(duration, start + window)
        out.append(TranscriptSegment(start=round(start, 3), end=round(end, 3), text=""))
        if end >= duration:
            break
        current += step
    return out


def compute_audio_energy_per_candidate(
    video_clip: Any,
    segments: list[TranscriptSegment],
) -> list[float]:
    if not segments:
        return []
    audio_clip = getattr(video_clip, "audio", None)
    if audio_clip is None:
        return [0.0 for _ in segments]
    try:
        import numpy as np  # type: ignore
    except Exception:
        return [0.0 for _ in segments]

    energies: list[float] = []
    for segment in segments:
        duration = max(0.05, segment.end - segment.start)
        try:
            probe = audio_clip.subclip(segment.start, segment.end).to_soundarray(fps=4000)
            if probe is None:
                energies.append(0.0)
                continue
            if getattr(probe, "size", 0) == 0:
                energies.append(0.0)
                continue
            if len(probe.shape) > 1:
                probe = np.mean(probe, axis=1)
            rms = float(np.sqrt(np.mean(np.square(probe))))
            energies.append(clamp(rms * 4.0, 0.0, 1.0))
        except Exception:
            energies.append(clamp(0.03 / duration, 0.0, 0.25))
    return energies


def score_candidates(
    segments: list[TranscriptSegment],
    duration: float,
    sentiment: SentimentScorer,
    audio_energies: list[float],
) -> list[MomentCandidate]:
    texts = [segment.text for segment in segments]
    sentiments = sentiment.score_many(texts)
    candidates: list[MomentCandidate] = []

    for idx, segment in enumerate(segments):
        text = segment.text
        compact = text.lower()
        sentiment_label, sentiment_intensity = sentiments[idx] if idx < len(sentiments) else ("neutral", 0.0)
        surprise_hits = count_keyword_hits(compact, SURPRISE_KEYWORDS)
        intrigue_hits = count_keyword_hits(compact, INTRIGUE_KEYWORDS)
        humor_hits = count_keyword_hits(compact, HUMOR_KEYWORDS)
        cliff_hits = count_keyword_hits(compact, CLIFFHANGER_KEYWORDS)
        question = 1.0 if "?" in compact else 0.0
        exclaim = clamp(compact.count("!") * 0.25, 0.0, 1.0)
        caps_ratio = clamp(
            sum(1 for token in text.split() if len(token) >= 3 and token.isupper()) / max(1, len(text.split())),
            0.0,
            1.0,
        )
        early_boost = 1.0 - clamp(segment.start / max(duration, 1e-6), 0.0, 1.0)
        late_boost = clamp(segment.end / max(duration, 1e-6), 0.0, 1.0)
        audio_energy = audio_energies[idx] if idx < len(audio_energies) else 0.0

        surprise_score = clamp((surprise_hits * 0.35) + (exclaim * 0.25) + (sentiment_intensity * 0.25) + (caps_ratio * 0.15), 0.0, 1.0)
        intrigue_score = clamp((intrigue_hits * 0.38) + (question * 0.28) + (cliff_hits * 0.2) + (sentiment_intensity * 0.14), 0.0, 1.0)
        humor_score = clamp((humor_hits * 0.55) + (exclaim * 0.1) + (audio_energy * 0.2) + (caps_ratio * 0.15), 0.0, 1.0)
        cliff_score = clamp((cliff_hits * 0.5) + (question * 0.2) + (intrigue_hits * 0.2) + (late_boost * 0.1), 0.0, 1.0)

        hook_score = clamp(
            (surprise_score * 0.34)
            + (intrigue_score * 0.22)
            + (sentiment_intensity * 0.18)
            + (audio_energy * 0.14)
            + (early_boost * 0.12),
            0.0,
            1.0,
        )
        dopamine_score = clamp(
            (surprise_score * 0.3)
            + (intrigue_score * 0.2)
            + (humor_score * 0.18)
            + (sentiment_intensity * 0.18)
            + (audio_energy * 0.14),
            0.0,
            1.0,
        )
        ending_score = clamp(
            (cliff_score * 0.45)
            + (intrigue_score * 0.2)
            + (sentiment_intensity * 0.1)
            + (audio_energy * 0.1)
            + (late_boost * 0.15),
            0.0,
            1.0,
        )

        candidates.append(
            MomentCandidate(
                start=segment.start,
                end=segment.end,
                text=text,
                sentiment_label=sentiment_label,
                sentiment_intensity=round(sentiment_intensity, 4),
                surprise_score=round(surprise_score, 4),
                intrigue_score=round(intrigue_score, 4),
                humor_score=round(humor_score, 4),
                cliff_score=round(cliff_score, 4),
                audio_energy=round(audio_energy, 4),
                hook_score=round(hook_score, 4),
                dopamine_score=round(dopamine_score, 4),
                ending_score=round(ending_score, 4),
            )
        )
    return candidates


def build_tease_window(start: float, end: float, target_len: float, duration: float) -> tuple[float, float]:
    if duration <= 0:
        return (0.0, 0.0)
    seg_center = (start + end) / 2.0
    length = clamp(target_len, 0.6, duration)
    tease_start = clamp(seg_center - (length / 2.0), 0.0, max(0.0, duration - length))
    tease_end = tease_start + length
    return (round(tease_start, 3), round(tease_end, 3))


def rating_from_score(score: float) -> float:
    return round(clamp(1.0 + (score * 9.0), 1.0, 10.0), 1)


def make_reason(candidate: MomentCandidate, role: str) -> str:
    if role == "hook":
        return (
            f"High surprise ({candidate.surprise_score:.2f}) and intrigue ({candidate.intrigue_score:.2f}) "
            f"with sentiment intensity {candidate.sentiment_intensity:.2f}."
        )
    if role == "dopamine":
        return (
            f"Strong dopamine mix: surprise {candidate.surprise_score:.2f}, intrigue {candidate.intrigue_score:.2f}, "
            f"audio energy {candidate.audio_energy:.2f}."
        )
    return (
        f"Ending tension score {candidate.ending_score:.2f} with cliff signal {candidate.cliff_score:.2f} "
        f"and late-position weight."
    )


def choose_hook(
    candidates: list[MomentCandidate],
    duration: float,
    hook_min: float,
    hook_max: float,
) -> PlannedMoment:
    if not candidates:
        return PlannedMoment(
            id="hook",
            role="hook",
            source_start=0.0,
            source_end=0.0,
            tease_start=0.0,
            tease_end=0.0,
            rating=1.0,
            reason="No candidate moments available.",
            overlay_text="Wait for it...",
            transcript_excerpt="",
        )
    best = max(candidates, key=lambda row: row.hook_score)
    tease_len = clamp(max(hook_min, best.end - best.start), hook_min, min(hook_max, max(duration, hook_min)))
    tease_start, tease_end = build_tease_window(best.start, best.end, tease_len, duration)
    return PlannedMoment(
        id="hook",
        role="hook",
        source_start=round(best.start, 3),
        source_end=round(best.end, 3),
        tease_start=tease_start,
        tease_end=tease_end,
        rating=rating_from_score(best.hook_score),
        reason=make_reason(best, "hook"),
        overlay_text="Wait for it...",
        transcript_excerpt=best.text[:220],
    )


def choose_ending_cliffhanger(
    candidates: list[MomentCandidate],
    duration: float,
    hook_min: float,
    hook_max: float,
) -> PlannedMoment:
    if not candidates:
        return PlannedMoment(
            id="ending_cliffhanger",
            role="ending_cliffhanger",
            source_start=0.0,
            source_end=0.0,
            tease_start=0.0,
            tease_end=0.0,
            rating=1.0,
            reason="No candidate moments available.",
            overlay_text="Part 2 changes everything...",
            transcript_excerpt="",
        )
    tail_candidates = [row for row in candidates if row.center >= duration * 0.65] or candidates
    best = max(tail_candidates, key=lambda row: (row.ending_score, row.center))
    tease_len = clamp(max(hook_min, best.end - best.start), hook_min, min(hook_max, max(duration, hook_min)))
    tease_start, tease_end = build_tease_window(best.start, best.end, tease_len, duration)
    return PlannedMoment(
        id="ending_cliffhanger",
        role="ending_cliffhanger",
        source_start=round(best.start, 3),
        source_end=round(best.end, 3),
        tease_start=tease_start,
        tease_end=tease_end,
        rating=rating_from_score(best.ending_score),
        reason=make_reason(best, "ending"),
        overlay_text="But this is only the beginning...",
        transcript_excerpt=best.text[:220],
    )


def overlap_seconds(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def choose_dopamine_hits(
    candidates: list[MomentCandidate],
    duration: float,
    count: int,
    spacing_min: float,
    hook: PlannedMoment,
    ending: PlannedMoment,
) -> list[PlannedMoment]:
    if not candidates or count <= 0:
        return []

    adaptive_spacing = spacing_min
    if duration < spacing_min * 1.5:
        adaptive_spacing = max(8.0, duration / max(2.0, count + 1))

    ranked = sorted(candidates, key=lambda row: row.dopamine_score, reverse=True)
    selected: list[MomentCandidate] = []
    for candidate in ranked:
        if len(selected) >= count:
            break
        if overlap_seconds(candidate.start, candidate.end, hook.source_start, hook.source_end) > 0.5:
            continue
        if overlap_seconds(candidate.start, candidate.end, ending.source_start, ending.source_end) > 0.5:
            continue
        if any(abs(candidate.center - existing.center) < adaptive_spacing for existing in selected):
            continue
        selected.append(candidate)

    if len(selected) < count:
        anchors = [((idx + 1) / (count + 1)) * duration for idx in range(count)]
        for anchor in anchors:
            if len(selected) >= count:
                break
            nearest = min(candidates, key=lambda row: abs(row.center - anchor))
            if any(abs(nearest.center - existing.center) < max(4.0, adaptive_spacing * 0.45) for existing in selected):
                continue
            if overlap_seconds(nearest.start, nearest.end, hook.source_start, hook.source_end) > 0.5:
                continue
            if overlap_seconds(nearest.start, nearest.end, ending.source_start, ending.source_end) > 0.5:
                continue
            selected.append(nearest)

    selected.sort(key=lambda row: row.start)
    hits: list[PlannedMoment] = []
    for idx, candidate in enumerate(selected[:count], start=1):
        tease_len = clamp(max(8.0, candidate.end - candidate.start), 8.0, min(12.0, max(duration, 8.0)))
        tease_start, tease_end = build_tease_window(candidate.start, candidate.end, tease_len, duration)
        hits.append(
            PlannedMoment(
                id=f"dopamine_hit_{idx:02d}",
                role="dopamine_hit",
                source_start=round(candidate.start, 3),
                source_end=round(candidate.end, 3),
                tease_start=tease_start,
                tease_end=tease_end,
                rating=rating_from_score(candidate.dopamine_score),
                reason=make_reason(candidate, "dopamine"),
                overlay_text="Stay with this...",
                transcript_excerpt=candidate.text[:220],
            )
        )
    return hits


def ffmpeg_clip_command(
    input_path: Path,
    output_path: Path,
    start: float,
    end: float,
    overlay_text: str,
    sfx_swell: Path | None,
) -> str:
    duration = max(0.2, end - start)
    draw_text = escape_drawtext(overlay_text)
    video_filter = (
        f"drawtext=text='{draw_text}':x=(w-text_w)/2:y=h*0.08:fontcolor=white:fontsize=56:"
        "box=1:boxcolor=black@0.45:boxborderw=16"
    )
    if sfx_swell is None or not sfx_swell.exists():
        return (
            f'ffmpeg -y -ss {start:.3f} -t {duration:.3f} -i "{safe_relpath_label(input_path)}" '
            f'-vf "{video_filter}" -c:v libx264 -crf 20 -preset medium -c:a aac '
            f'"{safe_relpath_label(output_path)}"'
        )
    fade_out_start = max(0.0, duration - 0.35)
    filter_complex = (
        f"[0:v]{video_filter}[v];"
        f"[1:a]atrim=0:{duration:.3f},afade=t=in:st=0:d=0.2,afade=t=out:st={fade_out_start:.3f}:d=0.3,volume=0.38[sfx];"
        f"[0:a][sfx]amix=inputs=2:duration=first:normalize=0[a]"
    )
    return (
        f'ffmpeg -y -ss {start:.3f} -t {duration:.3f} -i "{safe_relpath_label(input_path)}" -i "{safe_relpath_label(sfx_swell)}" '
        f'-filter_complex "{filter_complex}" -map "[v]" -map "[a]" -c:v libx264 -crf 20 -preset medium -c:a aac '
        f'"{safe_relpath_label(output_path)}"'
    )


def build_edit_script(
    input_path: Path,
    output_dir: Path,
    hook: PlannedMoment,
    dopamine_hits: list[PlannedMoment],
    ending: PlannedMoment,
    sfx_swell: Path | None,
) -> dict[str, Any]:
    clips_dir = output_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    ordered = [hook] + dopamine_hits + [ending]
    commands: list[str] = []
    clip_paths: list[Path] = []

    for moment in ordered:
        clip_path = clips_dir / f"{moment.id}.mp4"
        clip_paths.append(clip_path)
        commands.append(
            ffmpeg_clip_command(
                input_path=input_path,
                output_path=clip_path,
                start=moment.tease_start,
                end=moment.tease_end,
                overlay_text=moment.overlay_text,
                sfx_swell=sfx_swell,
            )
        )

    concat_list = output_dir / "concat_list.txt"
    concat_lines: list[str] = []
    for path in clip_paths:
        safe_path = str(path).replace("'", "'\\''")
        concat_lines.append(f"file '{safe_path}'")
    concat_list.write_text("\n".join(concat_lines), encoding="utf-8")

    final_cut_path = output_dir / "binge_tease_cut.mp4"
    commands.append(
        f'ffmpeg -y -f concat -safe 0 -i "{safe_relpath_label(concat_list)}" '
        f'-c:v libx264 -crf 20 -preset medium -c:a aac "{safe_relpath_label(final_cut_path)}"'
    )

    moviepy_steps = [
        "from moviepy.editor import VideoFileClip, concatenate_videoclips",
        f'source = VideoFileClip(r"{safe_relpath_label(input_path)}")',
        "clips = [",
    ]
    for moment in ordered:
        moviepy_steps.append(
            f"    source.subclip({moment.tease_start:.3f}, {moment.tease_end:.3f}),  # {moment.id}"
        )
    moviepy_steps += [
        "]",
        "final = concatenate_videoclips(clips, method='compose')",
        f'final.write_videofile(r"{safe_relpath_label(final_cut_path)}", codec="libx264", audio_codec="aac")',
    ]

    return {
        "commands": commands,
        "concatListPath": str(concat_list),
        "finalCutPath": str(final_cut_path),
        "moviepyBlueprint": moviepy_steps,
    }


def resolve_format(mode: str, duration: float) -> str:
    if mode in {"short", "long"}:
        return mode
    return "short" if duration <= 90.0 else "long"


def resolve_dopamine_count(format_mode: str, duration: float, requested_count: int | None) -> int:
    if requested_count is not None and requested_count > 0:
        return requested_count
    if format_mode == "short":
        return 3 if duration <= 40.0 else 4
    return int(clamp(round(duration / 150.0), 5, 10))


def analyze_video_and_build_plan(
    video_path: Path,
    transcript_path: Path | None,
    format_mode: str,
    hook_min: float,
    hook_max: float,
    dopamine_spacing_min: float,
    dopamine_count: int | None,
    sfx_swell: Path | None,
    output_dir: Path,
    sentiment_model: str,
) -> dict[str, Any]:
    try:
        from moviepy.editor import VideoFileClip  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"moviepy_import_failed:{exc}") from exc

    with VideoFileClip(str(video_path)) as clip:
        duration = float(getattr(clip, "duration", 0.0) or 0.0)
        if duration <= 0:
            raise RuntimeError("invalid_video_duration")

        resolved_format = resolve_format(format_mode, duration)
        segments = parse_transcript_segments(transcript_path, duration)
        transcript_present = bool(segments)
        if not segments:
            segments = build_synthetic_segments(duration, min_len=3.0, max_len=min(14.0, max(4.0, hook_max)))

        sentiment = SentimentScorer(model_name=sentiment_model)
        audio_energies = compute_audio_energy_per_candidate(clip, segments)
        candidates = score_candidates(segments, duration, sentiment, audio_energies)
        if not candidates:
            raise RuntimeError("no_candidates_generated")

        hook = choose_hook(candidates, duration, hook_min, hook_max)
        ending = choose_ending_cliffhanger(candidates, duration, hook_min, hook_max)
        hit_count = resolve_dopamine_count(resolved_format, duration, dopamine_count)
        dopamine_hits = choose_dopamine_hits(
            candidates=candidates,
            duration=duration,
            count=hit_count,
            spacing_min=dopamine_spacing_min,
            hook=hook,
            ending=ending,
        )

        edit_script = build_edit_script(
            input_path=video_path,
            output_dir=output_dir,
            hook=hook,
            dopamine_hits=dopamine_hits,
            ending=ending,
            sfx_swell=sfx_swell,
        )

        candidate_dump = []
        for candidate in sorted(candidates, key=lambda row: row.hook_score, reverse=True)[:40]:
            candidate_dump.append(asdict(candidate))

        avg_dopamine = 0.0
        if dopamine_hits:
            avg_dopamine = sum(hit.rating for hit in dopamine_hits) / len(dopamine_hits)

        return {
            "video": {
                "path": str(video_path),
                "durationSeconds": round(duration, 3),
                "format": resolved_format,
            },
            "inputs": {
                "transcriptPath": str(transcript_path) if transcript_path else None,
                "transcriptPresent": transcript_present,
                "sentimentBackend": sentiment.backend,
                "sentimentModel": sentiment.model_name,
                "sentimentBackendError": sentiment.error,
                "hookTeaseRangeSeconds": [hook_min, hook_max],
                "dopamineSpacingMinSeconds": dopamine_spacing_min,
                "dopamineTargetCount": hit_count,
            },
            "moments": {
                "hook": asdict(hook),
                "dopamineHits": [asdict(hit) for hit in dopamine_hits],
                "endingCliffhanger": asdict(ending),
            },
            "scores": {
                "hookRating": hook.rating,
                "averageDopamineRating": round(avg_dopamine, 2),
                "endingRating": ending.rating,
            },
            "editScript": edit_script,
            "topCandidates": candidate_dump,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build hook/dopamine/cliffhanger edit plan from video + transcript.")
    parser.add_argument("--input", required=True, help="Input video path.")
    parser.add_argument("--output-dir", required=True, help="Output directory for plan + command artifacts.")
    parser.add_argument("--transcript-json", default="", help="Optional transcript JSON path.")
    parser.add_argument("--format", default="auto", choices=["auto", "short", "long"], help="Planning mode.")
    parser.add_argument("--hook-min", default=8.0, type=float, help="Minimum hook tease length in seconds.")
    parser.add_argument("--hook-max", default=15.0, type=float, help="Maximum hook tease length in seconds.")
    parser.add_argument(
        "--dopamine-spacing-min",
        default=60.0,
        type=float,
        help="Preferred minimum spacing between dopamine hits (seconds).",
    )
    parser.add_argument("--dopamine-count", default=0, type=int, help="Optional explicit dopamine hit count.")
    parser.add_argument("--sfx-swell", default="", help="Optional SFX swell file mixed under teases.")
    parser.add_argument(
        "--sentiment-model",
        default="distilbert-base-uncased-finetuned-sst-2-english",
        help="Transformers sentiment model name.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    transcript_path = Path(args.transcript_json).expanduser().resolve() if str(args.transcript_json).strip() else None
    sfx_swell = Path(args.sfx_swell).expanduser().resolve() if str(args.sfx_swell).strip() else None
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(json.dumps({"ok": False, "error": "missing_input", "input": str(input_path)}))
        return 2

    hook_min = clamp(float(args.hook_min), 4.0, 20.0)
    hook_max = clamp(float(args.hook_max), hook_min, 30.0)
    dopamine_spacing_min = clamp(float(args.dopamine_spacing_min), 4.0, 600.0)
    dopamine_count = int(args.dopamine_count) if int(args.dopamine_count or 0) > 0 else None

    try:
        plan = analyze_video_and_build_plan(
            video_path=input_path,
            transcript_path=transcript_path,
            format_mode=str(args.format),
            hook_min=hook_min,
            hook_max=hook_max,
            dopamine_spacing_min=dopamine_spacing_min,
            dopamine_count=dopamine_count,
            sfx_swell=sfx_swell,
            output_dir=output_dir,
            sentiment_model=str(args.sentiment_model).strip(),
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"analysis_failed:{exc}"}))
        return 3

    plan_path = output_dir / "binge_edit_plan.json"
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    commands_path = output_dir / "ffmpeg_commands.txt"
    commands = plan.get("editScript", {}).get("commands", [])
    if isinstance(commands, list):
        commands_path.write_text("\n\n".join(str(command) for command in commands), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "planPath": str(plan_path),
                "commandsPath": str(commands_path),
                "hook": plan.get("moments", {}).get("hook"),
                "dopamineHitCount": len(plan.get("moments", {}).get("dopamineHits", [])),
                "endingCliffhanger": plan.get("moments", {}).get("endingCliffhanger"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
