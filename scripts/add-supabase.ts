import fs from 'fs'
import path from 'path'

const base = path.join(__dirname, '..')

function safeWrite(rel: string, content: string) {
  const p = path.join(base, rel)
  if (fs.existsSync(p)) {
    console.log('Exists, skipping', rel)
    return
  }
  fs.mkdirSync(path.dirname(p), { recursive: true })
  fs.writeFileSync(p, content)
  console.log('Created', rel)
}

safeWrite('src/routes/me.ts', `import express from 'express'
import { requireAuth } from '../middleware/requireAuth'
import { prisma } from '../db/prisma'

const router = express.Router()
router.get('/', requireAuth, async (req: any, res) => {
  const id = req.user?.sub
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const user = await prisma.user.findUnique({ where: { id } })
  res.json({ user })
})

export default router
`)

console.log('Supabase scaffolding complete')
