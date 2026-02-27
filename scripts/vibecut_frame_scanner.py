#!/usr/bin/env python3
"""
OpenCV frame scanner for VibeCut orientation/sub-mode recommendations.
Samples ~10% of frames by stride and emits aggregate signals as JSON.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def fallback_payload() -> dict:
    return {
        "sampledFrames": 0,
        "sampleStride": 0,
        "portraitSignal": 0.5,
        "landscapeSignal": 0.5,
        "centeredFaceVerticalSignal": 0.0,
        "horizontalMotionSignal": 0.0,
        "highMotionShortClipSignal": 0.0,
        "motionPeaks": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan video frames for orientation and retention hints.")
    parser.add_argument("--input", required=True, help="Input video path")
    parser.add_argument("--sample-ratio", default=0.1, type=float, help="Fraction of frames to sample")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        print(json.dumps(fallback_payload()))
        return 0

    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except Exception:
        print(json.dumps(fallback_payload()))
        return 0

    cap = cv2.VideoCapture(str(input_path))
    if not cap.isOpened():
        print(json.dumps(fallback_payload()))
        return 0

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)

    if total_frames <= 0 or width <= 0 or height <= 0:
        cap.release()
        print(json.dumps(fallback_payload()))
        return 0

    sample_ratio = clamp(float(args.sample_ratio or 0.1), 0.01, 0.5)
    sample_count = max(1, int(total_frames * sample_ratio))
    sample_stride = max(1, total_frames // sample_count)

    face_detector = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

    sampled_frames = 0
    portrait_score_total = 0.0
    landscape_score_total = 0.0
    centered_face_total = 0.0
    horizontal_motion_total = 0.0

    motion_values: list[float] = []
    motion_timestamps: list[float] = []

    prev_gray = None
    frame_index = 0

    while frame_index < total_frames:
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
        ok, frame = cap.read()
        if not ok or frame is None:
            frame_index += sample_stride
            continue

        sampled_frames += 1
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 80, 190)

        h, w = gray.shape
        x_mid = w // 2
        y_mid = h // 2

        left_density = float(edges[:, :x_mid].mean() / 255.0)
        right_density = float(edges[:, x_mid:].mean() / 255.0)
        top_density = float(edges[:y_mid, :].mean() / 255.0)
        bottom_density = float(edges[y_mid:, :].mean() / 255.0)

        center_x1 = int(w * 0.25)
        center_x2 = int(w * 0.75)
        center_y1 = int(h * 0.18)
        center_y2 = int(h * 0.85)
        center_density = float(edges[center_y1:center_y2, center_x1:center_x2].mean() / 255.0)

        portrait_bias = 1.0 if h >= w else 0.0
        landscape_bias = 1.0 if w > h else 0.0

        faces = face_detector.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=4, minSize=(36, 36))
        centered_face = 0.0
        if len(faces) > 0:
            hits = 0
            for (x, y, fw, fh) in faces:
                cx = x + fw / 2.0
                cy = y + fh / 2.0
                if 0.28 * w <= cx <= 0.72 * w and 0.18 * h <= cy <= 0.8 * h:
                    hits += 1
            centered_face = clamp(hits / max(1, len(faces)), 0.0, 1.0)

        portrait_score = (
            0.38 * portrait_bias
            + 0.26 * center_density
            + 0.22 * centered_face
            + 0.14 * (top_density + bottom_density) * 0.5
        )
        landscape_score = (
            0.42 * landscape_bias
            + 0.27 * ((left_density + right_density) * 0.5)
            + 0.18 * abs(left_density - right_density)
            + 0.13 * (1.0 - centered_face)
        )

        portrait_score_total += portrait_score
        landscape_score_total += landscape_score
        centered_face_total += centered_face

        motion_value = 0.0
        if prev_gray is not None:
            diff = cv2.absdiff(prev_gray, gray)
            motion_value = float(diff.mean() / 255.0)
            horizontal_band = diff[:, int(w * 0.2) : int(w * 0.8)]
            horizontal_motion_total += float(horizontal_band.mean() / 255.0)

        motion_values.append(motion_value)
        motion_timestamps.append(frame_index / max(1.0, fps))

        prev_gray = gray
        frame_index += sample_stride

    cap.release()

    if sampled_frames <= 0:
        print(json.dumps(fallback_payload()))
        return 0

    motion_array = np.array(motion_values, dtype=float) if motion_values else np.zeros((1,), dtype=float)
    motion_mean = float(motion_array.mean())
    motion_std = float(motion_array.std())
    high_motion_threshold = motion_mean + 0.75 * motion_std
    high_motion_ratio = float((motion_array > high_motion_threshold).mean()) if motion_array.size > 0 else 0.0

    peak_pairs = [
        (motion_timestamps[idx], float(value))
        for idx, value in enumerate(motion_values)
        if value >= high_motion_threshold
    ]
    peak_pairs.sort(key=lambda item: item[1], reverse=True)

    unique_peaks: list[float] = []
    for ts, _score in peak_pairs:
        if any(abs(ts - existing) < 4.5 for existing in unique_peaks):
            continue
        unique_peaks.append(ts)
        if len(unique_peaks) >= 6:
            break

    portrait_signal = clamp(portrait_score_total / sampled_frames, 0.0, 1.0)
    landscape_signal = clamp(landscape_score_total / sampled_frames, 0.0, 1.0)
    centered_face_signal = clamp(centered_face_total / sampled_frames, 0.0, 1.0)
    horizontal_motion_signal = clamp(horizontal_motion_total / max(1, sampled_frames - 1), 0.0, 1.0)
    high_motion_short_clip_signal = clamp(high_motion_ratio * 1.35, 0.0, 1.0)

    payload = {
        "sampledFrames": int(sampled_frames),
        "sampleStride": int(sample_stride),
        "portraitSignal": round(portrait_signal, 4),
        "landscapeSignal": round(landscape_signal, 4),
        "centeredFaceVerticalSignal": round(centered_face_signal, 4),
        "horizontalMotionSignal": round(horizontal_motion_signal, 4),
        "highMotionShortClipSignal": round(high_motion_short_clip_signal, 4),
        "motionPeaks": [round(value, 2) for value in unique_peaks],
    }

    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
