import express from 'express'
import fs from 'fs'
import path from 'path'

type AudioAssetType = 'sfx' | 'bgm'

type AudioAsset = {
  name: string
  displayName: string
  url: string
  type: AudioAssetType
}

const router = express.Router()

const SUPPORTED_AUDIO_EXTENSIONS = new Set(['.mp3', '.wav'])

const toDisplayName = (fileName: string) => {
  return fileName
    .replace(/\.[^/.]+$/, '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

const isSupportedAudioFile = (fileName: string) => {
  const ext = path.extname(fileName).toLowerCase()
  return SUPPORTED_AUDIO_EXTENSIONS.has(ext)
}

const resolveAssetsPublicDir = () => {
  const envOverride = String(process.env.AUDIO_ASSETS_PUBLIC_DIR || '').trim()
  const candidates = [
    envOverride
      ? (path.isAbsolute(envOverride) ? envOverride : path.resolve(process.cwd(), envOverride))
      : '',
    path.resolve(process.cwd(), 'public'),
    path.resolve(process.cwd(), 'frontend', 'public'),
    path.resolve(process.cwd(), '..', 'frontend', 'public')
  ].filter(Boolean)

  for (const candidate of candidates) {
    const sfxDir = path.join(candidate, 'sound-effects')
    const bgmDir = path.join(candidate, 'background-music')
    if (fs.existsSync(sfxDir) && fs.existsSync(bgmDir)) {
      return candidate
    }
  }
  return null
}

const readAssetDirectory = (
  dirPath: string,
  urlPrefix: '/sound-effects' | '/background-music',
  type: AudioAssetType
): AudioAsset[] => {
  if (!fs.existsSync(dirPath)) return []

  const entries = fs.readdirSync(dirPath, { withFileTypes: true })
  return entries
    .filter((entry) => entry.isFile() && isSupportedAudioFile(entry.name))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b))
    .map((fileName) => ({
      name: fileName,
      displayName: toDisplayName(fileName),
      url: `${urlPrefix}/${encodeURIComponent(fileName)}`,
      type
    }))
}

router.get('/', (_req, res) => {
  try {
    const publicDir = resolveAssetsPublicDir()
    if (!publicDir) {
      return res.status(500).json({ error: 'Failed to load audio assets' })
    }

    const soundEffects = readAssetDirectory(
      path.join(publicDir, 'sound-effects'),
      '/sound-effects',
      'sfx'
    )
    const backgroundMusic = readAssetDirectory(
      path.join(publicDir, 'background-music'),
      '/background-music',
      'bgm'
    )

    return res.status(200).json({
      soundEffects,
      backgroundMusic
    })
  } catch (error) {
    console.error('audio-assets route failed', error)
    return res.status(500).json({ error: 'Failed to load audio assets' })
  }
})

export default router
