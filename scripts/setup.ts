import { execSync } from 'child_process'
import path from 'path'

const base = path.join(__dirname, '..')

try {
  execSync('ts-node scripts/add-db.ts', { stdio: 'inherit', cwd: base })
  execSync('ts-node scripts/add-supabase.ts', { stdio: 'inherit', cwd: base })
  execSync('ts-node scripts/add-stripe.ts', { stdio: 'inherit', cwd: base })
  execSync('npm run prisma:seed', { stdio: 'inherit', cwd: base })
  console.log('Setup complete')
} catch (e) {
  console.error('Setup failed', e)
  process.exit(1)
}
