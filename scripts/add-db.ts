import { execSync } from 'child_process'
import fs from 'fs'
import path from 'path'

const base = path.join(__dirname, '..')

if (!fs.existsSync(path.join(base, 'prisma', 'schema.prisma'))) {
  console.error('Missing prisma/schema.prisma')
  process.exit(1)
}

try {
  console.log('Running prisma generate...')
  execSync('npm run prisma:generate', { stdio: 'inherit', cwd: base })
  console.log('Running prisma migrate dev --name init')
  execSync('npm run prisma:migrate', { stdio: 'inherit', cwd: base })
  console.log('Prisma migrated')
} catch (e) {
  console.error('Prisma commands failed', e)
  process.exit(1)
}
