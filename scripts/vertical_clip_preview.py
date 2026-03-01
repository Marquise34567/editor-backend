#!/usr/bin/env python3
"""
Desktop preview launcher for AutoEditor vertical clips.

Opens one window per rendered clip and loops playback. Each window includes:
- Play/Pause toggle
- Export button to save/copy the individual clip

This is intended for local desktop workflows (not headless production servers).
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import List

import cv2

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
except Exception as exc:  # pragma: no cover - GUI availability depends on host
    print(json.dumps({"ok": False, "error": f"tkinter_unavailable:{exc}"}))
    raise SystemExit(0)

try:
    from PIL import Image, ImageTk
except Exception as exc:  # pragma: no cover - optional dependency at runtime
    print(json.dumps({"ok": False, "error": f"pillow_unavailable:{exc}"}))
    raise SystemExit(0)


def parse_clip_paths(raw: str) -> List[Path]:
    try:
        payload = json.loads(raw)
    except Exception:
        payload = []
    if not isinstance(payload, list):
        return []
    clip_paths: List[Path] = []
    for item in payload:
        if not isinstance(item, str):
            continue
        candidate = Path(item).expanduser().resolve()
        if candidate.exists() and candidate.is_file():
            clip_paths.append(candidate)
    return clip_paths


class ClipPreviewWindow:
    def __init__(self, root: tk.Tk, clip_path: Path, index: int):
        self.root = root
        self.clip_path = clip_path
        self.index = index
        self.cap = cv2.VideoCapture(str(clip_path))
        self.playing = True
        self.closed = False
        self.frame_image = None

        fps = float(self.cap.get(cv2.CAP_PROP_FPS) or 0.0)
        self.delay_ms = max(15, int(1000 / (fps if fps > 0 else 30)))
        width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 360)
        height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 640)

        self.window = tk.Toplevel(root)
        self.window.title(f"AutoEditor Vertical Clip {index + 1}")
        self.window.geometry(f"{min(460, max(320, width // 2))}x{min(860, max(560, height // 2 + 84))}")
        self.window.protocol("WM_DELETE_WINDOW", self.close)

        self.video_label = tk.Label(self.window, bg="#0D1117")
        self.video_label.pack(fill=tk.BOTH, expand=True, padx=8, pady=(8, 6))

        controls = tk.Frame(self.window)
        controls.pack(fill=tk.X, padx=8, pady=(0, 8))

        self.play_btn = tk.Button(controls, text="Pause", width=10, command=self.toggle_play)
        self.play_btn.pack(side=tk.LEFT)

        export_btn = tk.Button(controls, text="Export Clip", width=14, command=self.export_clip)
        export_btn.pack(side=tk.LEFT, padx=(8, 0))

        name_label = tk.Label(
            controls,
            text=clip_path.name,
            anchor="e",
            justify=tk.RIGHT,
            fg="#9CA3AF",
        )
        name_label.pack(side=tk.RIGHT, fill=tk.X, expand=True)

    def start(self):
        self.schedule_next_tick(0)

    def schedule_next_tick(self, delay: int | None = None):
        if self.closed:
            return
        self.window.after(self.delay_ms if delay is None else delay, self.tick)

    def tick(self):
        if self.closed:
            return
        if not self.playing:
            self.schedule_next_tick()
            return

        ok, frame = self.cap.read()
        if not ok:
            self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ok, frame = self.cap.read()
        if ok and frame is not None:
            self.render_frame(frame)
        self.schedule_next_tick()

    def render_frame(self, frame):
        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        frame_h, frame_w = rgb.shape[:2]
        label_w = max(240, self.video_label.winfo_width())
        label_h = max(426, self.video_label.winfo_height())
        scale = min(label_w / frame_w, label_h / frame_h)
        out_w = max(1, int(frame_w * scale))
        out_h = max(1, int(frame_h * scale))
        resized = cv2.resize(rgb, (out_w, out_h), interpolation=cv2.INTER_AREA)
        pil = Image.fromarray(resized)
        self.frame_image = ImageTk.PhotoImage(image=pil)
        self.video_label.configure(image=self.frame_image)

    def toggle_play(self):
        self.playing = not self.playing
        self.play_btn.configure(text="Pause" if self.playing else "Play")

    def export_clip(self):
        initial_name = self.clip_path.name
        destination = filedialog.asksaveasfilename(
            title=f"Export Clip {self.index + 1}",
            initialfile=initial_name,
            defaultextension=".mp4",
            filetypes=[("MP4 video", "*.mp4"), ("All files", "*.*")],
        )
        if not destination:
            return
        try:
            shutil.copy2(self.clip_path, destination)
            messagebox.showinfo("Export complete", f"Saved clip to:\n{destination}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export clip:\n{exc}")

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.cap:
                self.cap.release()
        finally:
            try:
                self.window.destroy()
            except Exception:
                pass


def launch_preview_windows(clips: List[Path]) -> int:
    if not clips:
        print(json.dumps({"ok": False, "error": "no_valid_clips"}))
        return 0

    root = tk.Tk()
    root.withdraw()

    windows: List[ClipPreviewWindow] = []

    def monitor_windows():
        alive = [win for win in windows if not win.closed]
        if not alive:
            try:
                root.destroy()
            except Exception:
                pass
            return
        root.after(200, monitor_windows)

    for index, clip_path in enumerate(clips):
        preview = ClipPreviewWindow(root, clip_path, index)
        windows.append(preview)
        preview.start()

    root.after(200, monitor_windows)
    print(json.dumps({"ok": True, "windows": len(windows)}))
    root.mainloop()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Open looping desktop previews for rendered vertical clips.")
    parser.add_argument("--clips-json", required=True, help="JSON array of absolute clip paths.")
    args = parser.parse_args()

    clips = parse_clip_paths(args.clips_json)
    return launch_preview_windows(clips)


if __name__ == "__main__":
    raise SystemExit(main())

