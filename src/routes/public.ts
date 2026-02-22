import express from 'express'
import { getFounderAvailability } from '../services/founder'

const router = express.Router()

router.get('/founder', async (_req, res) => {
  try {
    const availability = await getFounderAvailability()
    res.json(availability)
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
