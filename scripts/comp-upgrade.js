const { prisma } = require('../src/db/prisma')

const email = process.argv[2]
if (!email) {
  console.error('Usage: node comp-upgrade.js email@example.com')
  process.exit(2)
}

async function run() {
  try {
    const e = String(email).toLowerCase()
    let user = await prisma.user.findUnique({ where: { email: e } })
    if (!user) {
      user = await prisma.user.create({ data: { email: e, planStatus: 'active' } })
      console.log('Created user', user.id)
    }
    await prisma.user.update({ where: { id: user.id }, data: { planStatus: 'active' } })
    try {
      await prisma.userSettings.upsert({ where: { userId: user.id }, update: { watermarkEnabled: false, exportQuality: '4k', autoZoomMax: 1.15 }, create: { userId: user.id, watermarkEnabled: false, exportQuality: '4k', autoZoomMax: 1.15 } })
    } catch (e) {
      // ignore
    }
    try {
      await prisma.subscription.upsert({ where: { userId: user.id }, update: { status: 'active', planTier: 'founder', stripeSubscriptionId: 'comped', stripeCustomerId: null, currentPeriodEnd: null }, create: { userId: user.id, status: 'active', planTier: 'founder', stripeSubscriptionId: 'comped', stripeCustomerId: null } })
    } catch (e) {
      // ignore
    }
    try {
      await prisma.adminAudit.create({ data: { actor: 'script', action: 'comp_upgrade', targetEmail: e, planKey: 'founder', reason: 'one-off admin script' } })
    } catch (e) {
      // ignore
    }
    console.log('Upgraded', e)
  } catch (err) {
    console.error('failed', err)
    process.exit(1)
  }
}
run()
