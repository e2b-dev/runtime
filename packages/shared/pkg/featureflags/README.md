# LaunchDarkly client authentication

The client selects authentication in this order:

1. A non-empty `LAUNCH_DARKLY_API_KEY` selects SDK-key authentication and ignores
   the OAuth variables.
2. Without an SDK key, OAuth variables select OAuth client-credentials authentication.
   Incomplete OAuth configuration fails startup.
3. Without either, the client uses the offline flag store.

For direct LaunchDarkly access, set `LAUNCH_DARKLY_API_KEY`. An optional
`LAUNCH_DARKLY_BASE_URL` redirects streaming, polling, events, and diagnostics
to a Relay proxy.

For an OAuth gateway, leave `LAUNCH_DARKLY_API_KEY` unset and configure:

| Variable | Value |
| --- | --- |
| `LAUNCH_DARKLY_BASE_URL` | HTTPS gateway URL |
| `LAUNCH_DARKLY_OAUTH_TOKEN_URL` | HTTPS OAuth token endpoint |
| `LAUNCH_DARKLY_OAUTH_CLIENT_ID` | Deployment client ID |
| `LAUNCH_DARKLY_OAUTH_CLIENT_SECRET` | Deployment client secret |
| `LAUNCH_DARKLY_OAUTH_AUDIENCE` | Optional audience parameter for the token request |

Use the existing deployment OAuth credentials and audience when the gateway
accepts that identity. The token endpoint must support the `client_credentials`
grant with credentials in the request body.

Both client constructors enable live flag updates without an SDK key in OAuth
mode.
OAuth URLs must not contain embedded credentials, query parameters, or fragments.

The client caches tokens and refreshes them before expiry. Every new HTTP
request uses the current bearer token, including stream reconnections, events,
and diagnostics. The gateway must bound stream duration because an open stream
does not reauthenticate when its token expires.

Token requests have a five-second timeout. OAuth requests do not follow
redirects. Authentication failures do not switch between SDK-key and OAuth modes.

The client mints its first token at startup. If the token endpoint answers with
a non-5xx response, construction fails, including with `WithStartOnInitTimeout`.
If the token endpoint is unreachable or returns 5xx, the SDK retries and the
existing initialization timeout policy applies.

A gateway `401` or `403` before the gateway has accepted any request stops the
SDK, as a rejected SDK key does. After the first accepted request, the client
drops its cached token on `401` or `403` and the SDK retries with a fresh one.
The SDK's own backoff bounds the retry and mint rate.
