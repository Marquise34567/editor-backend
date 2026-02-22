import { loadEnv } from '../src/lib/loadEnv'
import { supabaseAdmin } from '../src/supabaseClient'
import { getMonthKey } from '../src/shared/planConfig'

loadEnv()

const run = async () => {
  const currentMonth = getMonthKey()
  const { error } = await supabaseAdmin.from('usage').delete().neq('month', currentMonth)
  if (error) {
    console.error('usage reset failed', error)
    process.exit(1)
  }
  console.log(`usage reset complete for ${currentMonth}`)
}

run().catch((err) => {
  console.error('usage reset crashed', err)
  process.exit(1)
})
