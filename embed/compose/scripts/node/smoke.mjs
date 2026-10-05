import { Sandbox } from 'e2b'

const PORT = 8080
const REACH_RETRY_MS = 500
const REACH_TOTAL_MS = 15_000

for (const name of ['E2B_API_URL', 'E2B_API_KEY', 'E2B_SANDBOX_URL']) {
  if (!process.env[name]) {
    console.error(`smoke: ${name} is required`)
    console.error('FIX: export the E2B_API_URL, E2B_SANDBOX_URL and E2B_API_KEY values the ready container prints')
    process.exit(2)
  }
}

const sandboxUrl = process.env.E2B_SANDBOX_URL

// Built once, before anything is created: a value that is not a URL is a
// configuration error to report now, not something to retry against for 15 s
// and then blame on client-proxy.
let sandboxTarget
try {
  sandboxTarget = new URL('/', sandboxUrl)
} catch {
  console.error(`smoke: E2B_SANDBOX_URL is not a URL: ${sandboxUrl}`)
  console.error('FIX: export the E2B_API_URL, E2B_SANDBOX_URL and E2B_API_KEY values the ready container prints')
  process.exit(2)
}

// A failed fetch says only "fetch failed"; the connection error that names the
// port and the reason is the cause underneath it.
function reasonOf(err) {
  if (!(err instanceof Error)) return String(err)
  return err.cause instanceof Error ? `${err.message}: ${err.cause.message}` : err.message
}

// `getHost(port)` returns `{port}-{id}.e2b.app`, which nothing here resolves.
// An exposed port is reached through client-proxy instead, which routes on the
// two headers below, so that is the path this checks. The listener needs a
// moment to bind and client-proxy a moment to learn the sandbox, so this
// retries rather than sending one request. No attempt starts past the deadline
// and each one is bounded by the time left, so a request that never answers
// cannot stretch the 15 s this reports by more than the floor below.
async function reachPort(sandboxId) {
  const deadline = Date.now() + REACH_TOTAL_MS
  let last = 'no attempt completed'
  while (Date.now() < deadline) {
    try {
      const res = await fetch(sandboxTarget, {
        headers: { 'E2b-Sandbox-Id': sandboxId, 'E2b-Sandbox-Port': String(PORT) },
        signal: AbortSignal.timeout(Math.max(deadline - Date.now(), REACH_RETRY_MS)),
      })
      // Reading the body out releases the connection, so node can exit.
      await res.text()
      if (res.status === 200) return null
      last = `status ${res.status}`
    } catch (err) {
      last = reasonOf(err)
    }
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, Math.min(REACH_RETRY_MS, deadline - Date.now()))))
  }
  return last
}

let sbx
let listener
let failed = false
let portUnreachable = false
try {
  sbx = await Sandbox.create('base', { timeoutMs: 120_000 })
  const result = await sbx.commands.run('echo hello')
  if (result.exitCode !== 0 || result.stdout.trim() !== 'hello') {
    throw new Error(`unexpected command result: ${JSON.stringify(result)}`)
  }
  listener = await sbx.commands.run(`python3 -m http.server ${PORT} --bind 0.0.0.0`, { background: true })
  const reason = await reachPort(sbx.sandboxId)
  if (reason !== null) {
    console.error(`smoke: port ${PORT} was not reached through ${sandboxUrl} within ${REACH_TOTAL_MS / 1000}s: ${reason}`)
    failed = true
    portUnreachable = true
    process.exitCode = 1
  } else {
    console.log(`smoke: ok sandbox=${sbx.sandboxId} port ${PORT} reached through ${sandboxUrl}`)
  }
} catch (err) {
  console.error('smoke: failed:', err)
  failed = true
  process.exitCode = 1
} finally {
  // Ending the sandbox is part of what this test checks, so a kill failure is
  // still a failure: the exit code stays non-zero. It must read as a FIX line
  // though, never as an unhandled rejection out of the finally block. The FIX
  // lines are printed here, after the kill, so that when the test itself
  // failed its own FIX line is the last thing on stderr.
  if (listener) {
    // Best effort, and never a failure of its own: this drops the local stream
    // carrying the listener's output, which is what lets node exit once the
    // sandbox is gone. The command itself goes with the sandbox.
    await listener.disconnect().catch(() => {})
  }
  let killFailed = false
  if (sbx) {
    try {
      await sbx.kill()
    } catch (err) {
      console.error('smoke: the sandbox could not be killed:', err)
      killFailed = true
      process.exitCode = 1
    }
  }
  if (portUnreachable) {
    console.error('FIX: the exposed port could not be reached; read the client-proxy and orchestrator logs')
  } else if (failed) {
    console.error('FIX: read the api, orchestrator and client-proxy logs')
  } else if (killFailed) {
    console.error('FIX: the sandbox could not be killed; read the api and orchestrator logs')
  }
}
