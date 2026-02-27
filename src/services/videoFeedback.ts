const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));
const clamp01 = (value: number) => clamp(Number.isFinite(value) ? value : 0, 0, 1);
const round = (value: number, digits = 2) => Number((Number.isFinite(value) ? value : 0).toFixed(digits));
const toNum = (value: unknown, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

export type VideoFeedbackInput = {
  sourceType?: "classic" | "vibecut" | "upload";
  title?: string | null;
  rawDurationSeconds?: number | null;
  finalDurationSeconds?: number | null;
  deadAirRemovedSeconds?: number | null;
  hook?: {
    startSeconds?: number | null;
    endSeconds?: number | null;
    score?: number | null;
    confidence?: number | null;
    source?: string | null;
  } | null;
  audioEnhancements?: {
    chain?: string[];
    noiseReductionLevel?: number | null;
    eqApplied?: boolean;
  } | null;
  captions?: {
    enabled?: boolean;
    accuracyPercent?: number | null;
    timestamped?: boolean;
  } | null;
  templateUsed?: string | null;
  exportFormat?: {
    container?: string | null;
    resolution?: string | null;
    orientation?: "vertical" | "horizontal" | "unknown";
    aspectRatio?: string | null;
  } | null;
  manualEditTimeMinutes?: number | null;
  aiProcessTimeMinutes?: number | null;
  estimatedManualWithoutAiMinutes?: number | null;
  chapterCount?: number | null;
  engagement?: {
    views?: number | null;
    retentionRatePercent?: number | null;
    likes?: number | null;
    comments?: number | null;
    shares?: number | null;
    likesPerView?: number | null;
    commentsPerView?: number | null;
    sharesPerView?: number | null;
  } | null;
  trendSignals?: {
    tiktokShortBoost?: number | null;
    youtubeLongBoost?: number | null;
    instagramCaptionBoost?: number | null;
  } | null;
};

export type VideoFeedbackOutput = Record<string, any>;

const ratio = (value: unknown) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n >= 0 && n <= 1) return clamp01(n);
  return clamp01(n / 100);
};

const pct = (value: unknown) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n >= 0 && n <= 1) return round(n * 100, 2);
  return round(clamp(n, 0, 100), 2);
};

const classify = (seconds: number): "short-form" | "long-form" => {
  if (seconds <= 60) return "short-form";
  if (seconds >= 300) return "long-form";
  return Math.abs(seconds - 60) <= Math.abs(seconds - 300) ? "short-form" : "long-form";
};

const platformLabel = (score: number): "low" | "moderate" | "high" => {
  if (score >= 72) return "high";
  if (score >= 46) return "moderate";
  return "low";
};

const normalizeContrib = (items: Array<{ feature: string; value: number }>) => {
  const total = items.reduce((sum, item) => sum + Math.max(0, item.value), 0);
  if (total <= 0) return items.map((item) => ({ feature: item.feature, percent: 0 }));
  return items
    .map((item) => ({ feature: item.feature, percent: round((Math.max(0, item.value) / total) * 100, 1) }))
    .sort((a, b) => b.percent - a.percent);
};

const parseAudioScore = (chain: string[], noiseLevel: number, eqApplied: boolean) => {
  const chainSignal = Math.min(50, chain.length * 12);
  const noiseSignal = clamp(noiseLevel, 0, 100) * 0.34;
  const eqSignal = eqApplied ? 14 : 0;
  const denoiseHint = chain.some((x) => /afftdn|arnndn|denoise/i.test(x)) ? 9 : 0;
  const loudHint = chain.some((x) => /loudnorm|acompressor|compand/i.test(x)) ? 11 : 0;
  return round(clamp(18 + chainSignal + noiseSignal + eqSignal + denoiseHint + loudHint, 0, 100), 2);
};

const deriveCaptionAccuracy = (enabled: boolean, provided: number | null, duration: number, chapters: number, hook: number, chainCount: number) => {
  if (!enabled) return 0;
  if (provided !== null) return round(clamp(provided, 0, 100), 2);
  const durationDensity = duration > 0 ? clamp01(120 / Math.max(120, duration)) : 0.45;
  const chapterSignal = Math.min(0.16, chapters * 0.025);
  const hookSignal = hook * 0.14;
  const chainSignal = Math.min(0.14, chainCount * 0.03);
  const raw = 68 + durationDensity * 18 + chapterSignal * 100 + hookSignal * 100 + chainSignal * 100;
  return round(clamp(raw, 58, 98), 2);
};

const fallbackManualEstimate = (rawMin: number, kind: "short-form" | "long-form", removedRatio: number, hook: number, captions: boolean, audioCount: number, chapters: number) => {
  const complexity =
    0.44 +
    removedRatio * 1.25 +
    hook * 0.32 +
    (captions ? 0.22 : 0.08) +
    Math.min(0.26, audioCount * 0.06) +
    (kind === "long-form" ? 0.38 + Math.min(0.34, chapters * 0.05) : 0);
  return round(Math.max(rawMin * (1.2 + complexity), rawMin * 0.6), 2);
};

const fallbackActual = (rawMin: number, kind: "short-form" | "long-form", captions: boolean, audioCount: number) => {
  const effort = 0.22 + (kind === "long-form" ? 0.18 : 0.1) + (captions ? 0.08 : 0.03) + Math.min(0.2, audioCount * 0.04);
  return round(Math.max(1.2, rawMin * effort), 2);
};

export const buildVideoFeedbackAnalysis = async (input: VideoFeedbackInput): Promise<VideoFeedbackOutput> => {
  const sourceType = input.sourceType === "vibecut" || input.sourceType === "upload" ? input.sourceType : "classic";
  const title = String(input.title || "Untitled video").trim() || "Untitled video";

  const rawSeconds = Math.max(1, toNum(input.rawDurationSeconds, 0));
  const finalSeconds = clamp(toNum(input.finalDurationSeconds, rawSeconds), Math.max(1, rawSeconds * 0.05), rawSeconds);
  const removedSeconds = clamp(toNum(input.deadAirRemovedSeconds, rawSeconds - finalSeconds), 0, rawSeconds);

  const kind = classify(rawSeconds);
  const rawMinutes = rawSeconds / 60;
  const removedRatio = clamp01(removedSeconds / Math.max(1, rawSeconds));
  const shortenedPercent = round(clamp01((rawSeconds - finalSeconds) / Math.max(1, rawSeconds)) * 100, 2);

  const hookScore = clamp01(ratio(input.hook?.score) ?? 0);
  const hookConfidence = clamp01(ratio(input.hook?.confidence) ?? hookScore);
  const hookStrength = Math.max(hookScore, hookConfidence);

  const chain = Array.isArray(input.audioEnhancements?.chain)
    ? input.audioEnhancements!.chain!.map((x) => String(x || "").trim()).filter(Boolean)
    : [];
  const noiseLevel = clamp(toNum(input.audioEnhancements?.noiseReductionLevel, 0), 0, 100);
  const eqApplied = Boolean(input.audioEnhancements?.eqApplied) || chain.some((x) => /eq|treble|highpass|lowpass|equalizer/i.test(x));
  const audioScore = parseAudioScore(chain, noiseLevel, eqApplied);

  const chapters = Math.max(0, Math.round(toNum(input.chapterCount, 0)));
  const captionsEnabled = Boolean(input.captions?.enabled);
  const captionAccuracy = deriveCaptionAccuracy(captionsEnabled, pct(input.captions?.accuracyPercent), rawSeconds, chapters, hookStrength, chain.length);

  const manualEditMin = Math.max(0, toNum(input.manualEditTimeMinutes, 0));
  const aiProcessMin = Math.max(0, toNum(input.aiProcessTimeMinutes, 0));
  const manualWithoutAi = toNum(input.estimatedManualWithoutAiMinutes, 0) > 0
    ? round(toNum(input.estimatedManualWithoutAiMinutes, 0), 2)
    : fallbackManualEstimate(rawMinutes, kind, removedRatio, hookStrength, captionsEnabled, chain.length, chapters);

  const actualSpent = round(
    Math.max(0.5, (aiProcessMin > 0 ? aiProcessMin : fallbackActual(rawMinutes, kind, captionsEnabled, chain.length)) + manualEditMin),
    2
  );
  const savedMinutes = round(Math.max(0, manualWithoutAi - actualSpent), 2);
  const aiContributionPercent = round(clamp01(savedMinutes / Math.max(manualWithoutAi, 0.01)) * 100, 2);

  const deadAirDriver = round(removedRatio * (kind === "short-form" ? 28 : 20), 2);
  const hookDriver = round(hookStrength * (kind === "short-form" ? 24 : 18), 2);
  const captionDriver = round(captionsEnabled ? (captionAccuracy / 100) * (kind === "short-form" ? 10 : 8) : 0, 2);
  const chapterDriver = round(kind === "long-form" ? Math.min(16, chapters * 2.2) : 0, 2);
  const retentionLift = round(clamp(deadAirDriver + hookDriver + captionDriver + chapterDriver, 0, 58), 2);

  const simulatedBefore = round(rawSeconds * clamp01((kind === "short-form" ? 0.5 : 0.38) + hookStrength * 0.15 + removedRatio * 0.08), 2);
  const simulatedAfter = round(clamp(simulatedBefore * (1 + retentionLift / 100), 0, rawSeconds), 2);

  const chapterBreakdown = kind === "long-form" && chapters > 0
    ? Array.from({ length: chapters }).map((_, i) => {
        const wave = Math.sin((i + 1) * 0.75 + hookStrength * 1.8) * 0.8;
        return { chapter: i + 1, estimatedLiftPercent: round(clamp(retentionLift / Math.max(1, chapters) + wave, 0, 12), 2) };
      })
    : [];

  const views = toNum(input.engagement?.views, Number.NaN);
  const likes = toNum(input.engagement?.likes, Number.NaN);
  const comments = toNum(input.engagement?.comments, Number.NaN);
  const shares = toNum(input.engagement?.shares, Number.NaN);

  const viewsResolved = Number.isFinite(views) && views >= 0 ? Math.round(views) : null;
  const likesPv = ratio(input.engagement?.likesPerView);
  const commentsPv = ratio(input.engagement?.commentsPerView);
  const sharesPv = ratio(input.engagement?.sharesPerView);

  const likesResolved = Number.isFinite(likes) && likes >= 0
    ? Math.round(likes)
    : (viewsResolved !== null && likesPv !== null ? Math.round(viewsResolved * likesPv) : null);
  const commentsResolved = Number.isFinite(comments) && comments >= 0
    ? Math.round(comments)
    : (viewsResolved !== null && commentsPv !== null ? Math.round(viewsResolved * commentsPv) : null);
  const sharesResolved = Number.isFinite(shares) && shares >= 0
    ? Math.round(shares)
    : (viewsResolved !== null && sharesPv !== null ? Math.round(viewsResolved * sharesPv) : null);

  const likeRate = viewsResolved && likesResolved !== null
    ? round((likesResolved / Math.max(1, viewsResolved)) * 100, 2)
    : likesPv !== null
      ? round(likesPv * 100, 2)
      : null;
  const commentRate = viewsResolved && commentsResolved !== null
    ? round((commentsResolved / Math.max(1, viewsResolved)) * 100, 2)
    : commentsPv !== null
      ? round(commentsPv * 100, 2)
      : null;
  const shareRate = viewsResolved && sharesResolved !== null
    ? round((sharesResolved / Math.max(1, viewsResolved)) * 100, 2)
    : sharesPv !== null
      ? round(sharesPv * 100, 2)
      : null;

  const engagementRate = (likeRate !== null || commentRate !== null || shareRate !== null)
    ? round((likeRate || 0) + (commentRate || 0) + (shareRate || 0), 2)
    : null;
  const retentionRate = pct(input.engagement?.retentionRatePercent);
  const hasEngagement = Boolean(retentionRate !== null || viewsResolved !== null || engagementRate !== null);

  const deadAirScore = round(removedRatio * 100, 2);
  const chapterScore = round(kind === "long-form" ? clamp(chapters * 18, 0, 100) : 0, 2);
  const baseEff = deadAirScore * 0.4 + audioScore * 0.3 + (captionsEnabled ? captionAccuracy : 0) * 0.3;
  const aiEffScore = round(kind === "long-form" ? baseEff * 0.8 + chapterScore * 0.2 : baseEff, 2);

  const contributions = normalizeContrib([
    { feature: "Dead-air trim", value: deadAirDriver },
    { feature: "Hook quality", value: hookDriver },
    { feature: "Audio polish", value: audioScore * 0.22 },
    { feature: "Caption clarity", value: captionDriver * 1.3 },
    { feature: "Chapter flow", value: chapterDriver }
  ]);
  const topFeature = contributions[0]?.feature || "Hook quality";

  const orientation = input.exportFormat?.orientation || "unknown";
  const aspectRatio = String(input.exportFormat?.aspectRatio || (orientation === "vertical" ? "9:16" : orientation === "horizontal" ? "16:9" : "unknown")).trim();
  const resolution = String(input.exportFormat?.resolution || "Auto").trim() || "Auto";
  const container = String(input.exportFormat?.container || "mp4").trim() || "mp4";

  const lenTikTok = finalSeconds <= 60 ? 1 : clamp01(1 - (finalSeconds - 60) / 240);
  const lenYoutube = finalSeconds >= 600
    ? 1
    : finalSeconds >= 300
      ? clamp01(0.72 + (finalSeconds - 300) / 900)
      : finalSeconds >= 60
        ? clamp01(0.38 + (finalSeconds - 60) / 420)
        : 0.22;
  const lenInstagram = clamp01((orientation === "vertical" ? 0.72 : 0.48) + (finalSeconds <= 90 ? 0.18 : 0));

  const retentionStrength = clamp01((retentionLift / 58) * 0.68 + hookStrength * 0.32);
  const engagementStrength = clamp01((retentionRate !== null ? retentionRate / 100 : simulatedAfter / Math.max(1, rawSeconds)));
  const captionStrength = captionsEnabled ? clamp01(captionAccuracy / 100) : 0.22;
  const chapterStrength = kind === "long-form" ? clamp01(chapters / Math.max(1, Math.round(finalSeconds / 240))) : 0;
  const audioStrength = clamp01(audioScore / 100);
  const pacingStrength = clamp01(removedRatio * 1.35 + (chain.length > 0 ? 0.12 : 0) + hookStrength * 0.08);

  const baseTikTok = 100 * (0.34 * lenTikTok + 0.28 * hookStrength + 0.18 * retentionStrength + 0.1 * (orientation === "vertical" ? 1 : 0.45) + 0.1 * captionStrength);
  const baseYoutube = 100 * (0.32 * lenYoutube + 0.22 * engagementStrength + 0.18 * retentionStrength + 0.16 * chapterStrength + 0.12 * audioStrength);
  const baseInstagram = 100 * (0.28 * lenInstagram + 0.22 * retentionStrength + 0.2 * captionStrength + 0.18 * pacingStrength + 0.12 * (orientation === "vertical" ? 1 : 0.58));

  const trendTikTok = clamp(toNum(input.trendSignals?.tiktokShortBoost, 1), 0.75, 2.6);
  const trendYoutube = clamp(toNum(input.trendSignals?.youtubeLongBoost, 1), 0.75, 2.6);
  const trendInstagram = clamp(toNum(input.trendSignals?.instagramCaptionBoost, 1), 0.75, 2.6);

  const scoreTikTok = round(clamp(baseTikTok * trendTikTok, 0, 100), 2);
  const scoreYoutube = round(clamp(baseYoutube * trendYoutube, 0, 100), 2);
  const scoreInstagram = round(clamp(baseInstagram * trendInstagram, 0, 100), 2);

  const signalCount = [rawSeconds > 0, finalSeconds > 0, removedSeconds >= 0, hookStrength > 0, chain.length > 0 || audioScore > 20, captionsEnabled, chapters > 0, hasEngagement].filter(Boolean).length;
  const confBase = clamp01(0.42 + signalCount * 0.06);

  const youtube = {
    score: scoreYoutube,
    confidence: round(clamp(100 * (confBase + (kind === "long-form" ? 0.1 : 0)), 40, 99), 2),
    potential: platformLabel(scoreYoutube),
    reasoning: `${kind === "long-form" ? "Long-form runtime" : "Mid/short runtime"} with ${chapters > 0 ? `${chapters} chapter signals` : "limited chapter structure"} and projected retention lift ${retentionLift.toFixed(1)}%.`
  };

  const tiktok = {
    score: scoreTikTok,
    confidence: round(clamp(100 * (confBase + (kind === "short-form" ? 0.1 : 0)), 40, 99), 2),
    potential: platformLabel(scoreTikTok),
    reasoning: `Duration ${Math.round(finalSeconds)}s + hook ${Math.round(hookStrength * 100)}% gives ${scoreTikTok >= 70 ? "strong" : "moderate"} short-form momentum.`
  };

  const instagram = {
    score: scoreInstagram,
    confidence: round(clamp(100 * confBase, 40, 98), 2),
    potential: platformLabel(scoreInstagram),
    reasoning: `${captionsEnabled ? "Captioned" : "Uncaptioned"} pacing with retention lift ${retentionLift.toFixed(1)}% supports Reel/feed scroll stops.`
  };

  const bestFit = ([
    ["youtube", youtube.score],
    ["tiktok", tiktok.score],
    ["instagram", instagram.score]
  ] as Array<["youtube" | "tiktok" | "instagram", number]>).sort((a, b) => b[1] - a[1])[0][0];

  const suggestions: string[] = [];
  if (kind === "short-form" && finalSeconds > 60) suggestions.push("Trim this cut under 60s to sharpen short-form distribution windows.");
  if (hookStrength < 0.72) suggestions.push("Strengthen the first 3-8 seconds with a clearer payoff statement and faster visual contrast.");
  if (!captionsEnabled) suggestions.push("Enable captions to capture silent viewers and improve scroll-stop comprehension.");
  if (kind === "long-form" && chapters < 3) suggestions.push("Add more chapter anchors to improve mid-video navigation and watch-time depth.");
  if (retentionLift < 14) suggestions.push("Increase pacing contrast in low-energy sections; compress setup beats and move payoff moments earlier.");

  const weakest = ([
    ["youtube", youtube.score],
    ["tiktok", tiktok.score],
    ["instagram", instagram.score]
  ] as Array<["youtube" | "tiktok" | "instagram", number]>).sort((a, b) => a[1] - b[1])[0][0];
  if (weakest !== bestFit) {
    if (weakest === "youtube") suggestions.push("For stronger YouTube performance, expand context continuity and reinforce segment transitions every 2-4 minutes.");
    if (weakest === "tiktok") suggestions.push("For better TikTok velocity, tighten opener pacing and remove any delay before the first visual payoff.");
    if (weakest === "instagram") suggestions.push("For Instagram uplift, prioritize visual rhythm and bold caption styling in the first two scene changes.");
  }
  if (!suggestions.length) suggestions.push("Current edit is well-balanced; run one variant with a stronger opening hook to test upside in distribution.");

  return {
    schemaVersion: 1,
    style: { surface: "dark", accent: "gold", renderHint: "premium_cards" },
    classification: {
      type: kind,
      reason: kind === "short-form" ? `Classified as short-form from runtime ${Math.round(rawSeconds)}s.` : `Classified as long-form from runtime ${Math.round(rawSeconds)}s.`
    },
    source: { type: sourceType, title },
    metrics: {
      timeSavedOnThisEdit: {
        estimatedManualWithoutAiMinutes: manualWithoutAi,
        actualTimeSpentMinutes: actualSpent,
        timeSavedMinutes: savedMinutes,
        aiContributionPercent,
        summary: `Saved ${savedMinutes.toFixed(1)} minutes versus manual-only editing for this exact video.`,
        visualization: "Progress bar: manual-only time vs AI+manual time",
        action: kind === "long-form" ? "Use the recovered time to tighten chapter intros and reinforce mid-roll hooks." : "Use the recovered time to test a second hook variant for short-form performance."
      },
      renderDetails: {
        exportFormat: { container, resolution, orientation, aspectRatio },
        templateUsed: String(input.templateUsed || (kind === "long-form" ? "Long-form adaptive narrative" : "Short-form retention cut")),
        chapterCount: chapters,
        featureCoveragePercent: {
          deadAirTrim: round(removedRatio * 100, 2),
          audioEnhancement: round(clamp01(audioScore / 100) * 100, 2),
          captions: captionsEnabled ? 100 : 0
        },
        summary: `Exported as ${container.toUpperCase()} ${resolution} (${aspectRatio}) using ${String(input.templateUsed || "adaptive template")}.`,
        visualization: kind === "long-form" ? "Icon summary with chapter chips" : "Compact icon summary row",
        action: kind === "long-form" ? "Add chapter labels in title/description for better navigation discoverability." : "Keep opener visual contrast high for the first 1-2 scene changes."
      },
      retentionPotential: {
        estimatedLiftPercent: retentionLift,
        driverBreakdown: { deadAirTrim: deadAirDriver, hookStrength: hookDriver, captionClarity: captionDriver, chapterFlow: chapterDriver },
        simulatedWatchTimeSeconds: { before: simulatedBefore, after: simulatedAfter },
        chapterBreakdown,
        summary: `Hook and pacing profile suggest ~${retentionLift.toFixed(1)}% retention upside on this cut.`,
        visualization: kind === "long-form" ? "Before/after line graph with chapter overlay" : "Before/after retention sparkline",
        action: kind === "long-form" ? "Focus on mid-video transitions to protect cumulative watch time." : "Front-load payoff language before second 5 for stronger hold."
      },
      engagementInsights: {
        available: hasEngagement,
        views: viewsResolved,
        retentionRatePercent: retentionRate,
        likes: likesResolved,
        comments: commentsResolved,
        shares: sharesResolved,
        ratios: { likeRatePercent: likeRate, commentRatePercent: commentRate, shareRatePercent: shareRate, engagementRatePercent: engagementRate },
        summary: hasEngagement
          ? `${retentionRate !== null ? `${retentionRate.toFixed(1)}% retention` : "Retention pending"} with ${engagementRate !== null ? `${engagementRate.toFixed(2)}% engagement` : "engagement ratios pending"} for this video.`
          : "No post-export engagement telemetry linked for this video yet.",
        visualization: hasEngagement ? "Timeline heatmap for drop-off + interaction ratios" : "Placeholder heatmap state until telemetry sync",
        action: hasEngagement
          ? (kind === "long-form" ? "If mid-video retention dips, insert tighter chapter transitions and recap hooks." : "If early retention softens, shorten setup and place payoff copy earlier.")
          : "Connect platform analytics to replace simulated estimates with live outcomes."
      },
      aiEfficiencyScore: {
        score: aiEffScore,
        weights: { deadAirDetection: 40, audioQuality: 30, captionAccuracy: 30, chapteringBonus: kind === "long-form" ? 20 : 0 },
        componentScores: { deadAirDetection: deadAirScore, audioQuality: audioScore, captionAccuracy: captionsEnabled ? captionAccuracy : 0, chaptering: chapterScore },
        summary: `AI efficiency score: ${aiEffScore.toFixed(1)}/100.`,
        visualization: "Gauge dial with weighted component bars",
        action: kind === "long-form" && chapterScore < 55 ? "Increase chapter structure depth to raise long-form efficiency." : "Improve lowest component to lift score on next render cycle."
      },
      contentOptimizationBreakdown: {
        rawLengthSeconds: round(rawSeconds, 2),
        finalLengthSeconds: round(finalSeconds, 2),
        shortenedPercent,
        mostImpactfulFeature: topFeature,
        featureContributionPercent: contributions,
        summary: `Runtime optimized by ${shortenedPercent.toFixed(1)}%, led by ${topFeature.toLowerCase()}.`,
        visualization: "Pie chart for feature contribution share",
        action: bestFit === "tiktok"
          ? "Lean further into hook pacing for stronger share velocity."
          : bestFit === "youtube"
            ? "Strengthen narrative continuity to convert retention into watch hours."
            : "Maintain visual cadence and caption readability for Reel distribution."
      },
      platformPerformancePredictions: {
        youtube,
        tiktok,
        instagram,
        bestFit,
        summary: `Best projected fit: ${bestFit === "youtube" ? "YouTube" : bestFit === "tiktok" ? "TikTok" : "Instagram"} based on runtime, hook strength, pacing, and retention signals from this video only.`,
        visualization: "Bar chart of platform scores with confidence badges",
        action: bestFit === "youtube"
          ? "Publish with chapter timestamps and a watch-time-first title strategy."
          : bestFit === "tiktok"
            ? "Publish as short vertical cut with immediate opener payoff and loop-ready ending."
            : "Publish as Reel with bold captions and first-frame visual contrast."
      },
      improvementSuggestions: {
        suggestions: suggestions.slice(0, 6),
        summary: `Top focus: ${(suggestions[0] || "Run one additional variant test to validate the strongest hook.")}`,
        visualization: "Checklist cards sorted by predicted impact"
      }
    },
    motivationalNote: `Strong ${topFeature.toLowerCase()} performance gives this edit real momentum. Best platform fit right now: ${bestFit === "youtube" ? "YouTube" : bestFit === "tiktok" ? "TikTok" : "Instagram"}.`
  };
};
