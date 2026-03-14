import nodemailer from 'nodemailer'
import type { Transporter } from 'nodemailer'

let transporter: Transporter | null = null

const initializeTransporter = (): Transporter => {
  const smtpHost = process.env.SMTP_HOST
  const smtpPort = process.env.SMTP_PORT ? parseInt(process.env.SMTP_PORT) : 587
  const smtpUser = process.env.SMTP_USER
  const smtpPass = process.env.SMTP_PASS
  const smtpFromEmail = process.env.SMTP_FROM_EMAIL || 'noreply@autoeditor.app'
  const smtpFromName = process.env.SMTP_FROM_NAME || 'AutoEditor'

  if (!smtpHost || !smtpUser || !smtpPass) {
    throw new Error('SMTP configuration missing: SMTP_HOST, SMTP_USER, and SMTP_PASS required')
  }

  return nodemailer.createTransport({
    host: smtpHost,
    port: smtpPort,
    secure: smtpPort === 465,
    auth: {
      user: smtpUser,
      pass: smtpPass
    }
  })
}

export const getTransporter = (): Transporter => {
  if (!transporter) {
    transporter = initializeTransporter()
  }
  return transporter
}

export const sendPasswordResetEmail = async (
  email: string,
  resetLink: string,
  expiryMinutes: number = 60
): Promise<{ success: boolean; error?: string }> => {
  try {
    const transporter = getTransporter()
    const smtpFromEmail = process.env.SMTP_FROM_EMAIL || 'noreply@autoeditor.app'
    const smtpFromName = process.env.SMTP_FROM_NAME || 'AutoEditor'
    const appUrl = process.env.APP_URL || process.env.FRONTEND_URL || 'https://www.autoeditor.app'

    const htmlContent = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; text-align: center; border-radius: 6px 6px 0 0;">
          <h1 style="color: white; margin: 0;">Password Reset Request</h1>
        </div>
        <div style="background: #f9f9f9; padding: 30px; border-radius: 0 0 6px 6px;">
          <p style="color: #333; font-size: 16px;">Hi,</p>
          <p style="color: #333; font-size: 16px;">We received a request to reset your AutoEditor password. Click the button below to set a new password.</p>
          <div style="text-align: center; margin: 30px 0;">
            <a href="${resetLink}" style="display: inline-block; background: #667eea; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">Reset Password</a>
          </div>
          <p style="color: #666; font-size: 14px;">Or copy and paste this link in your browser:</p>
          <p style="color: #666; font-size: 13px; word-break: break-all; background: #f0f0f0; padding: 10px; border-radius: 4px;">${resetLink}</p>
          <p style="color: #999; font-size: 13px; margin-top: 20px;">This link expires in ${expiryMinutes} minutes.</p>
          <p style="color: #999; font-size: 13px;">If you didn't request this, please ignore this email.</p>
          <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;" />
          <p style="color: #999; font-size: 12px; text-align: center;">© 2026 AutoEditor. All rights reserved.</p>
        </div>
      </div>
    `

    const textContent = `
Password Reset Request

We received a request to reset your AutoEditor password. Visit the link below to set a new password:

${resetLink}

This link expires in ${expiryMinutes} minutes.

If you didn't request this, please ignore this email.

© 2026 AutoEditor
    `

    await transporter.sendMail({
      from: `"${smtpFromName}" <${smtpFromEmail}>`,
      to: email,
      subject: 'Reset your AutoEditor password',
      text: textContent,
      html: htmlContent
    })

    return { success: true }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error)
    console.error('Failed to send password reset email:', errorMessage)
    return {
      success: false,
      error: errorMessage
    }
  }
}

export const sendEmailConfirmation = async (
  email: string,
  confirmLink: string,
  expiryMinutes: number = 24 * 60
): Promise<{ success: boolean; error?: string }> => {
  try {
    const transporter = getTransporter()
    const smtpFromEmail = process.env.SMTP_FROM_EMAIL || 'noreply@autoeditor.app'
    const smtpFromName = process.env.SMTP_FROM_NAME || 'AutoEditor'

    const htmlContent = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; text-align: center; border-radius: 6px 6px 0 0;">
          <h1 style="color: white; margin: 0;">Confirm Your Email</h1>
        </div>
        <div style="background: #f9f9f9; padding: 30px; border-radius: 0 0 6px 6px;">
          <p style="color: #333; font-size: 16px;">Hi,</p>
          <p style="color: #333; font-size: 16px;">Welcome to AutoEditor! Please confirm your email address to activate your account.</p>
          <div style="text-align: center; margin: 30px 0;">
            <a href="${confirmLink}" style="display: inline-block; background: #667eea; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">Confirm Email</a>
          </div>
          <p style="color: #666; font-size: 14px;">Or copy and paste this link in your browser:</p>
          <p style="color: #666; font-size: 13px; word-break: break-all; background: #f0f0f0; padding: 10px; border-radius: 4px;">${confirmLink}</p>
          <p style="color: #999; font-size: 13px; margin-top: 20px;">This link expires in ${expiryMinutes} minutes.</p>
          <p style="color: #999; font-size: 13px;">If you didn't create this account, please ignore this email.</p>
        </div>
      </div>
    `

    await transporter.sendMail({
      from: `"${smtpFromName}" <${smtpFromEmail}>`,
      to: email,
      subject: 'Confirm your AutoEditor email',
      html: htmlContent
    })

    return { success: true }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error)
    console.error('Failed to send email confirmation:', errorMessage)
    return {
      success: false,
      error: errorMessage
    }
  }
}
