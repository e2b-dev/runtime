import { Template } from 'e2b'

async function main() {
  const apiUrl = process.env.E2B_API_URL
  const apiKey = process.env.E2B_API_KEY
  if (!apiUrl || !apiKey) {
    console.error('build-base-template: E2B_API_URL and E2B_API_KEY are required')
    console.error('FIX: export the E2B_API_URL and E2B_API_KEY values the ready container prints')
    process.exit(2)
  }

  async function baseIsReady() {
    const res = await fetch(`${apiUrl}/templates`, { headers: { 'X-API-Key': apiKey } })
    if (!res.ok) throw new Error(`GET /templates failed with ${res.status}`)
    const templates = await res.json()
    return templates.some(
      (t) => [...(t.aliases ?? []), ...(t.names ?? [])].includes('base') && t.buildStatus === 'ready',
    )
  }

  if (process.env.FORCE_REBUILD !== '1' && (await baseIsReady())) {
    console.log('build-base-template: alias "base" already has a ready build; nothing to do')
    process.exit(0)
  }

  console.log('build-base-template: building alias "base" from e2bdev/base (512 MiB, 2 vCPU)')
  const started = Date.now()
  const RETRY_MS = 5_000
  const RETRY_TOTAL_MS = 120_000
  const isNodeRegistering = (err) => {
    const m = err instanceof Error ? err.message : String(err)
    return /\b503\b/.test(m) && /build client/i.test(m)
  }
  const deadline = Date.now() + RETRY_TOTAL_MS
  for (;;) {
    try {
      await Template.build(Template().fromBaseImage(), 'base', {
        memoryMB: 512,
        cpuCount: 2,
        onBuildLogs: (entry) => console.log(String(entry)),
      })
      break
    } catch (err) {
      if (!isNodeRegistering(err) || Date.now() >= deadline) throw err
      console.log(`build-base-template: orchestrator not registered with the api yet, retrying in ${RETRY_MS / 1000}s (${Math.round((deadline - Date.now()) / 1000)}s left)`)
      await new Promise((resolve) => setTimeout(resolve, RETRY_MS))
    }
  }
  console.log(`build-base-template: done in ${Math.round((Date.now() - started) / 1000)}s`)
}

main().catch((err) => {
  console.error(`build-base-template: ${err instanceof Error ? err.message : String(err)}`)
  console.error('FIX: read the api and orchestrator logs for the template-manager error, then start the stack again')
  process.exit(1)
})
