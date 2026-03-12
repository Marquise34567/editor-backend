#!/usr/bin/env python3
import argparse
import json
import math
import sys

import cv2

try:
    import mediapipe as mp  # type: ignore
except Exception:
    mp = None


CORNER_ANCHORS = (
    {"id": "top_left", "x": 0.08, "y": 0.08, "top": True, "left": True},
    {"id": "top_right", "x": 0.92, "y": 0.08, "top": True, "left": False},
    {"id": "bottom_left", "x": 0.08, "y": 0.92, "top": False, "left": True},
    {"id": "bottom_right", "x": 0.92, "y": 0.92, "top": False, "left": False},
)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def normalized_fallback_crop(width: int, height: int) -> dict:
    side = max(32, int(height / 3))
    side = min(side, width, height)
    return {
        "x": 0.0,
        "y": 0.0,
        "w": round(side / float(width), 6),
        "h": round(side / float(height), 6),
    }


def normalized_context_crop(
    *,
    center_x: float,
    center_y: float,
    side_length: float,
    width: int,
    height: int,
) -> dict:
    side = clamp(float(side_length), min(width, height) * 0.12, min(width, height) * 0.96)
    x1 = int(round(center_x - (side / 2.0)))
    y1 = int(round(center_y - (side / 2.0)))
    x1 = int(clamp(x1, 0, max(0, width - int(side))))
    y1 = int(clamp(y1, 0, max(0, height - int(side))))
    x2 = int(clamp(x1 + int(side), x1 + 1, width))
    y2 = int(clamp(y1 + int(side), y1 + 1, height))
    crop_w = max(1, x2 - x1)
    crop_h = max(1, y2 - y1)
    return {
        "x": round(x1 / float(width), 6),
        "y": round(y1 / float(height), 6),
        "w": round(crop_w / float(width), 6),
        "h": round(crop_h / float(height), 6),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MediaPipe corner-focused webcam crop scan")
    parser.add_argument("--input", required=True, help="Input video path")
    parser.add_argument("--sample-every", type=int, default=3, help="Process every Nth frame")
    parser.add_argument("--max-samples", type=int, default=420, help="Maximum sampled frames")
    parser.add_argument("--model-selection", type=int, default=1, help="MediaPipe model_selection")
    parser.add_argument("--min-confidence", type=float, default=0.5, help="MediaPipe min_detection_confidence")
    parser.add_argument(
        "--context-scale",
        type=float,
        default=2.8,
        help="Side length multiplier over detected face for webcam crop context",
    )
    return parser


def detect_faces_mediapipe(face_detector, frame_bgr, width: int, height: int):
    if face_detector is None:
        return []
    rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    results = face_detector.process(rgb)
    if not results or not results.detections:
        return []
    detections = []
    for det in results.detections:
        bbox = det.location_data.relative_bounding_box
        x = max(0.0, float(bbox.xmin) * width)
        y = max(0.0, float(bbox.ymin) * height)
        w = max(1.0, float(bbox.width) * width)
        h = max(1.0, float(bbox.height) * height)
        if x >= width or y >= height:
            continue
        w = min(w, width - x)
        h = min(h, height - y)
        if w <= 1 or h <= 1:
            continue
        score = 0.5
        try:
            if det.score:
                score = float(det.score[0])
        except Exception:
            score = 0.5
        detections.append(
            {
                "x": x,
                "y": y,
                "w": w,
                "h": h,
                "cx": x + (w / 2.0),
                "cy": y + (h / 2.0),
                "score": clamp(score, 0.05, 1.0),
                "detector": "mediapipe",
            }
        )
    return detections


def detect_faces_haar(haar_cascade, frame_bgr):
    if haar_cascade is None:
        return []
    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    faces = haar_cascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(24, 24),
    )
    detections = []
    for (x, y, w, h) in faces:
        if w <= 1 or h <= 1:
            continue
        detections.append(
            {
                "x": float(x),
                "y": float(y),
                "w": float(w),
                "h": float(h),
                "cx": float(x) + (float(w) / 2.0),
                "cy": float(y) + (float(h) / 2.0),
                "score": 0.45,
                "detector": "opencv_haar",
            }
        )
    return detections


def main() -> int:
    args = build_arg_parser().parse_args()
    sample_every = max(1, int(args.sample_every))
    max_samples = max(1, int(args.max_samples))
    context_scale = clamp(float(args.context_scale), 1.5, 4.0)
    min_confidence = clamp(float(args.min_confidence), 0.05, 0.99)

    cap = cv2.VideoCapture(args.input)
    if not cap.isOpened():
        print(json.dumps({"ok": False, "error": "video_open_failed"}))
        return 2

    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    if width <= 0 or height <= 0:
        cap.release()
        print(json.dumps({"ok": False, "error": "video_dimensions_unavailable"}))
        return 3

    face_detector = None
    detector_mode = "none"
    if mp is not None:
        try:
            face_detector = mp.solutions.face_detection.FaceDetection(
                model_selection=int(args.model_selection),
                min_detection_confidence=min_confidence,
            )
            detector_mode = "mediapipe"
        except Exception:
            face_detector = None

    haar_cascade = None
    if face_detector is None:
        try:
            cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
            if cascade_path:
                loaded = cv2.CascadeClassifier(cascade_path)
                if loaded is not None and not loaded.empty():
                    haar_cascade = loaded
                    detector_mode = "opencv_haar"
        except Exception:
            haar_cascade = None

    stats = {
        corner["id"]: {
            "score": 0.0,
            "weight": 0.0,
            "count": 0,
            "sum_cx": 0.0,
            "sum_cy": 0.0,
            "sum_side": 0.0,
        }
        for corner in CORNER_ANCHORS
    }
    sampled_frames = 0
    detected_frames = 0
    frame_index = -1
    total_frames_read = 0
    best_anywhere = None

    while cap.isOpened():
        ok, frame = cap.read()
        if not ok:
            break
        total_frames_read += 1
        frame_index += 1
        if frame_index % sample_every != 0:
            continue
        sampled_frames += 1
        if sampled_frames > max_samples:
            break

        detections = detect_faces_mediapipe(face_detector, frame, width, height)
        if not detections:
            detections = detect_faces_haar(haar_cascade, frame)
        if not detections:
            continue

        detected_frames += 1
        for det in detections:
            cx_n = clamp(det["cx"] / float(width), 0.0, 1.0)
            cy_n = clamp(det["cy"] / float(height), 0.0, 1.0)
            side = max(det["w"], det["h"]) * context_scale
            side = clamp(side, min(width, height) * 0.18, min(width, height) * 0.92)
            area_n = clamp((det["w"] * det["h"]) / float(max(1, width * height)), 0.0, 1.0)
            det_score = clamp(float(det["score"]), 0.05, 1.0)
            anywhere_score = det_score * (0.72 + min(1.0, area_n * 40.0) * 0.28)
            if best_anywhere is None or anywhere_score > float(best_anywhere["score"]):
                best_anywhere = {
                    "score": anywhere_score,
                    "cx": float(det["cx"]),
                    "cy": float(det["cy"]),
                    "side": float(side),
                    "detector": str(det.get("detector", detector_mode) or detector_mode),
                }

            for corner in CORNER_ANCHORS:
                dx = abs(cx_n - corner["x"])
                dy = abs(cy_n - corner["y"])
                distance = math.sqrt((dx / 0.75) ** 2 + (dy / 0.75) ** 2)
                proximity = clamp(1.0 - distance, 0.0, 1.0)
                if proximity <= 0.08:
                    continue
                top_bias = 1.08 if corner["top"] else 0.92
                weight = det_score * (0.55 + min(1.0, area_n * 40.0) * 0.45) * (proximity ** 2) * top_bias
                if weight <= 0.002:
                    continue
                entry = stats[corner["id"]]
                entry["score"] += weight
                entry["weight"] += weight
                entry["count"] += 1
                entry["sum_cx"] += det["cx"] * weight
                entry["sum_cy"] += det["cy"] * weight
                entry["sum_side"] += side * weight
                stats[corner["id"]] = entry

    cap.release()
    if face_detector is not None:
        try:
            face_detector.close()
        except Exception:
            pass

    ranked = sorted(
        (
            {
                "id": corner["id"],
                "score": stats[corner["id"]]["score"],
                "weight": stats[corner["id"]]["weight"],
                "count": stats[corner["id"]]["count"],
                "top": corner["top"],
                "left": corner["left"],
                "anchor_x": corner["x"],
                "anchor_y": corner["y"],
            }
            for corner in CORNER_ANCHORS
        ),
        key=lambda x: float(x["score"]),
        reverse=True,
    )
    best = ranked[0]
    total_score = sum(float(item["score"]) for item in ranked)

    if best["score"] > 0.0 and best["weight"] > 0.0 and best["count"] >= 2:
        entry = stats[best["id"]]
        mean_cx = entry["sum_cx"] / entry["weight"]
        mean_cy = entry["sum_cy"] / entry["weight"]
        mean_side = entry["sum_side"] / entry["weight"]

        x1 = int(round(mean_cx - (mean_side / 2.0)))
        y1 = int(round(mean_cy - (mean_side / 2.0)))
        x1 = int(clamp(x1, 0, max(0, width - int(mean_side))))
        y1 = int(clamp(y1, 0, max(0, height - int(mean_side))))
        x2 = int(clamp(x1 + int(mean_side), x1 + 1, width))
        y2 = int(clamp(y1 + int(mean_side), y1 + 1, height))
        crop_w = max(1, x2 - x1)
        crop_h = max(1, y2 - y1)
        crop = {
            "x": round(x1 / float(width), 6),
            "y": round(y1 / float(height), 6),
            "w": round(crop_w / float(width), 6),
            "h": round(crop_h / float(height), 6),
        }
        print(
            json.dumps(
                {
                    "ok": True,
                    "fallback": False,
                    "detector": detector_mode,
                    "corner": best["id"],
                    "confidence": round(best["score"] / max(0.0001, total_score), 4),
                    "sampledFrames": sampled_frames,
                    "detectedFrames": detected_frames,
                    "readFrames": total_frames_read,
                    "crop": crop,
                }
            )
        )
        return 0

    if best_anywhere is not None:
        crop = normalized_context_crop(
            center_x=float(best_anywhere["cx"]),
            center_y=float(best_anywhere["cy"]),
            side_length=float(best_anywhere["side"]),
            width=width,
            height=height,
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "fallback": False,
                    "detector": str(best_anywhere.get("detector", detector_mode) or detector_mode),
                    "corner": "anywhere_face",
                    "confidence": round(clamp(float(best_anywhere["score"]), 0.0, 1.0), 4),
                    "sampledFrames": sampled_frames,
                    "detectedFrames": detected_frames,
                    "readFrames": total_frames_read,
                    "crop": crop,
                }
            )
        )
        return 0

    print(
        json.dumps(
            {
                "ok": True,
                "fallback": True,
                "detector": detector_mode,
                "corner": "top_left",
                "confidence": 0.0,
                "sampledFrames": sampled_frames,
                "detectedFrames": detected_frames,
                "readFrames": total_frames_read,
                "crop": normalized_fallback_crop(width, height),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
