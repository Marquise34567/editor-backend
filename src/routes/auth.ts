import express from 'express'
import { sendPasswordResetEmail } from '../services/emailService'
import supabaseAdmin from '../supabaseClient'
import { prisma } from '../db/prisma'

const router = express.Router()

/**
 * POST /api/auth/password-reset/request
 * Request a password reset email
 * Body: { email: string }
 */
router.post('/password-reset/request', async (req, res) => {
  try {
    const email = typeof req.body?.email === 'string' ? req.body.email.trim().toLowerCase() : null

    if (!email || !email.includes('@')) {
      return res.status(400).json({ error: 'invalid_email' })
    }

    // Check rate limiting - only allow 1 reset email per email per hour
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000)
    const recentReset = await prisma.passwordReset.findFirst({
      where: {
        email,
        createdAt: { gte: oneHourAgo }
      }
    })

    if (recentReset) {
      return res.status(429).json({
        error: 'rate_limited',
        message: 'Please wait before requesting another reset email. Try again in a few minutes.'
      })
    }

    // Generate a reset token (Supabase handles this internally, but we can also generate our own)
    const resetToken = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15)
    const expiresAt = new Date(Date.now() + 60 * 60 * 1000) // 1 hour expiry

    // Store the reset token in database
    await prisma.passwordReset.create({
      data: {
        email,
        token: resetToken,
        expiresAt
      }
    })

    // Generate reset link
    const frontendUrl = (process.env.FRONTEND_URL || process.env.APP_URL || 'https://www.autoeditor.app').replace(/\/$/, '')
    const resetLink = `${frontendUrl}/reset-password?token=${resetToken}`

    // Send email
    const result = await sendPasswordResetEmail(email, resetLink, 60)

    if (!result.success) {
      return res.status(500).json({
        error: 'email_send_failed',
        message: 'Failed to send reset email. Please try again later.'
      })
    }

    return res.json({
      success: true,
      message: 'Password reset email sent. Check your inbox.'
    })
  } catch (error) {
    console.error('Password reset request failed:', error)
    return res.status(500).json({
      error: 'server_error',
      message: 'An unexpected error occurred. Please try again later.'
    })
  }
})

/**
 * POST /api/auth/password-reset/verify
 * Verify a password reset token and reset the password
 * Body: { token: string, password: string }
 */
router.post('/password-reset/verify', async (req, res) => {
  try {
    const token = typeof req.body?.token === 'string' ? req.body.token.trim() : null
    const password = typeof req.body?.password === 'string' ? req.body.password : null
    const email = typeof req.body?.email === 'string' ? req.body.email.trim().toLowerCase() : null

    if (!token || !password || !email) {
      return res.status(400).json({ error: 'missing_fields' })
    }

    if (password.length < 8) {
      return res.status(400).json({ error: 'password_too_short' })
    }

    // Check if token is valid and not expired
    const resetRecord = await prisma.passwordReset.findFirst({
      where: {
        email,
        token,
        expiresAt: { gt: new Date() }
      }
    })

    if (!resetRecord) {
      return res.status(400).json({
        error: 'invalid_token',
        message: 'Reset link expired or is invalid. Please request a new one.'
      })
    }

    // Update password via Supabase
    const { data, error } = await supabaseAdmin.auth.admin.updateUserById(email, {
      password
    })

    if (error) {
      console.error('Supabase password update failed:', error)
      return res.status(400).json({
        error: 'password_update_failed',
        message: error.message || 'Failed to update password'
      })
    }

    // Mark token as used
    await prisma.passwordReset.update({
      where: { id: resetRecord.id },
      data: { used: true, usedAt: new Date() }
    })

    return res.json({
      success: true,
      message: 'Password reset successful. Please sign in with your new password.'
    })
  } catch (error) {
    console.error('Password reset verification failed:', error)
    return res.status(500).json({
      error: 'server_error',
      message: 'An unexpected error occurred. Please try again later.'
    })
  }
})

export default router
