import { supabaseAdmin } from '../supabaseClient'
import { getMonthKey } from '../shared/planConfig'
import { getUsageForMonth } from './usage'

const isMissingUsageTable = (error: any) => {
  const code = error?.code || error?.details?.code
  const message = String(error?.message || '')
  return code === 'PGRST205' || message.includes("Could not find the table 'public.usage'")
}

export const getRenderUsageForMonth = async (
  userId: string,
  monthKey: string = getMonthKey()
) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('usage')
      .select('renders_count')
      .eq('user_id', userId)
      .eq('month', monthKey)
      .maybeSingle()
    if (error) {
      if (isMissingUsageTable(error)) {
        const usage = await getUsageForMonth(userId, monthKey)
        return { rendersCount: usage?.rendersUsed ?? 0 }
      }
      console.warn('usage lookup failed', error)
    }
    if (data && typeof data.renders_count === 'number') {
      return { rendersCount: data.renders_count }
    }
    const { data: created, error: upsertError } = await supabaseAdmin
      .from('usage')
      .upsert(
        { user_id: userId, month: monthKey, renders_count: 0, updated_at: new Date().toISOString() },
        { onConflict: 'user_id,month' }
      )
      .select('renders_count')
      .maybeSingle()
    if (upsertError) {
      if (isMissingUsageTable(upsertError)) {
        const usage = await getUsageForMonth(userId, monthKey)
        return { rendersCount: usage?.rendersUsed ?? 0 }
      }
      console.warn('usage upsert failed', upsertError)
      return { rendersCount: 0 }
    }
    return { rendersCount: typeof created?.renders_count === 'number' ? created.renders_count : 0 }
  } catch (err: any) {
    if (isMissingUsageTable(err)) {
      const usage = await getUsageForMonth(userId, monthKey)
      return { rendersCount: usage?.rendersUsed ?? 0 }
    }
    console.warn('usage tracking failed', err)
    return { rendersCount: 0 }
  }
}

export const incrementRenderUsage = async (
  userId: string,
  monthKey: string = getMonthKey(),
  delta: number = 1
) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('usage')
      .select('renders_count')
      .eq('user_id', userId)
      .eq('month', monthKey)
      .maybeSingle()
    if (error) {
      if (isMissingUsageTable(error)) return
      console.warn('usage lookup failed', error)
    }
    const current = Number(data?.renders_count ?? 0)
    const next = current + delta
    const { error: upsertError } = await supabaseAdmin
      .from('usage')
      .upsert({ user_id: userId, month: monthKey, renders_count: next, updated_at: new Date().toISOString() }, { onConflict: 'user_id,month' })
    if (upsertError) {
      if (isMissingUsageTable(upsertError)) return
      console.warn('usage upsert failed', upsertError)
    }
  } catch (err: any) {
    if (isMissingUsageTable(err)) return
    console.warn('usage tracking failed', err)
  }
}
