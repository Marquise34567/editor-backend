import fetch from 'node-fetch'

const DEFAULT_VIEW_EMOJI = '✅'
const DEFAULT_SUBJECT = `Your Video is Ready ${DEFAULT_VIEW_EMOJI}`
const DEFAULT_FROM = 'updates@autoeditor.app'

const normalizeEmail = (value: unknown) => String(value || '').trim().toLowerCase()
const isValidEmail = (value: unknown) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizeEmail(value))
const isPlaceholderEmail = (value: string) => value.endsWith('@autoeditor.local') || value.endsWith('@autoeditor.internal')

const buildBaseUrl = (raw: string | undefined) => {
  const trimmed = String(raw || '').trim()
  if (!trimmed) return null
  try {
    return new URL(trimmed)
  } catch {
    return null
  }
}

const resolveAppBaseUrl = () => {
  const appUrl = buildBaseUrl(process.env.APP_URL)
  if (appUrl) return appUrl
  const frontendUrl = buildBaseUrl(process.env.FRONTEND_URL)
  if (frontendUrl) return frontendUrl
  return buildBaseUrl('https://www.autoeditor.app')!
}

const resolveApiBaseUrl = () => {
  const apiUrl = buildBaseUrl(process.env.APP_BASE_URL)
  if (apiUrl) return apiUrl
  return resolveAppBaseUrl()
}

const toAbsoluteUrl = (value: string | null | undefined, baseUrl: URL) => {
  const raw = String(value || '').trim()
  if (!raw) return null
  try {
    return new URL(raw, baseUrl).toString()
  } catch {
    return raw
  }
}

const buildEditorUrl = (jobId?: string | null) => {
  const base = resolveAppBaseUrl()
  try {
    const url = new URL('/editor', base)
    if (jobId) url.searchParams.set('jobId', jobId)
    return url.toString()
  } catch {
    const fallbackBase = base.toString().replace(/\/$/, '')
    return jobId ? `${fallbackBase}/editor?jobId=${encodeURIComponent(jobId)}` : `${fallbackBase}/editor`
  }
}

type VideoReadyEmailPayload = {
  email: string | null | undefined
  jobId?: string | null
  title?: string | null
  outputUrl?: string | null
  editorUrl?: string | null
  emoji?: string | null
}

export const sendVideoReadyEmail = async (payload: VideoReadyEmailPayload) => {
  const normalizedEmail = normalizeEmail(payload.email)
  if (!isValidEmail(normalizedEmail) || isPlaceholderEmail(normalizedEmail)) {
    return { skipped: 'invalid_email' as const }
  }

  const webhookUrl = String(process.env.VIDEO_READY_EMAIL_WEBHOOK_URL || '').trim()
  const resendKey = String(process.env.RESEND_API_KEY || '').trim()
  if (!webhookUrl && !resendKey) {
    return { skipped: 'provider_not_configured' as const }
  }

  const emoji = String(payload.emoji || DEFAULT_VIEW_EMOJI).trim() || DEFAULT_VIEW_EMOJI
  const subject = String(process.env.VIDEO_READY_EMAIL_SUBJECT || DEFAULT_SUBJECT).trim() || DEFAULT_SUBJECT
  const from = String(process.env.VIDEO_READY_EMAIL_FROM || process.env.REPORTS_EMAIL_FROM || DEFAULT_FROM).trim()
  const editorUrl = payload.editorUrl || buildEditorUrl(payload.jobId)
  const absoluteOutputUrl = toAbsoluteUrl(payload.outputUrl, resolveApiBaseUrl())
  const absoluteEditorUrl = toAbsoluteUrl(editorUrl, resolveAppBaseUrl())
  const viewUrl = absoluteOutputUrl || absoluteEditorUrl
  if (!viewUrl) {
    return { skipped: 'missing_view_url' as const }
  }

  const label = String(payload.title || '').trim()
  const titleLine = label ? `Project: ${label}` : 'Your export finished successfully.'
  const heading = `Your Video is Ready ${emoji}`
  const text = [
    heading,
    '',
    titleLine,
    '',
    `View video: ${viewUrl}`
  ].join('\n')
  const html = `
    <div style="font-family:Arial,sans-serif;line-height:1.5;color:#111;">
      <h2 style="margin:0 0 12px 0;">${heading}</h2>
      <p style="margin:0 0 18px 0;">${titleLine}</p>
      <p style="margin:0 0 18px 0;">
        <a href="${viewUrl}" style="background:#111;color:#fff;text-decoration:none;padding:12px 18px;border-radius:8px;display:inline-block;">
          View Video
        </a>
      </p>
      <p style="margin:0;font-size:12px;color:#666;">If the button doesn't work, open: ${viewUrl}</p>
    </div>
  `.trim()

  if (webhookUrl) {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: normalizedEmail, subject, text, html })
    })
    if (!response.ok) {
      const body = await response.text().catch(() => '')
      throw new Error(`video_ready_webhook_failed:${response.status}:${body}`)
    }
    return { provider: 'webhook' as const }
  }

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${resendKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from,
      to: [normalizedEmail],
      subject,
      text,
      html
    })
  })
  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`video_ready_resend_failed:${response.status}:${body}`)
  }
  return { provider: 'resend' as const }
}
