# Browser Navigation API v1

Status: initial safe API layer for Fast Filings browser automation.

## Safety model

- Mounted at `/browser-nav`.
- All plan/run endpoints require the existing admin token header or bearer token:
  - `x-ff-sync-token: <token>` or `Authorization: Bearer <token>`
- Live browser execution is disabled unless `BROWSER_NAV_LIVE_ENABLED=true`.
- Browser profiles are configured only through environment JSON; no profile secrets or CDP URLs are hard-coded.
- Each request must include `agentId` and `profileId`.
- Default policy only enables Mark:
  - `agentId: "mark"`
  - profiles: `mark`, `mark-bb`
  - blocks Google Workspace browser navigation hosts for Mark (`mail.google.com`, `drive.google.com`, `sheets.google.com`, `docs.google.com`, `script.google.com`, `accounts.google.com`, etc.).
- Fill values are not written to audit logs. Audit records include only action type, selector, URL host/path, and text length.

## Environment

```bash
BROWSER_NAV_LIVE_ENABLED=false
BROWSER_NAV_AUDIT_STDOUT=true
BROWSER_NAV_MAX_STEPS=30
BROWSER_NAV_PROFILES_JSON='{
  "mark": { "cdpHttpUrl": "http://127.0.0.1:9222", "defaultUrl": "about:blank", "label": "Mark local Chrome" },
  "mark-bb": { "cdpHttpUrl": "https://...", "defaultUrl": "about:blank", "label": "Mark AZ Browserbase" }
}'
BROWSER_NAV_AGENT_POLICIES_JSON='{
  "mark": {
    "business": "Fast Filings",
    "allowedProfiles": ["mark", "mark-bb"],
    "blockedHosts": ["accounts.google.com", "mail.google.com", "drive.google.com", "sheets.google.com", "docs.google.com", "script.google.com"],
    "notes": "Fast Filings browser profile policy"
  }
}'
```

If `BROWSER_NAV_AGENT_POLICIES_JSON` is omitted, the default Mark/Fast Filings policy above is used.

## Endpoints

### `GET /browser-nav/health`

Returns route status, configured agents/profiles, live-execution state, and safety markers. Does not require browser access.

### `GET /browser-nav/runs`

Lists recent sanitized run records. Requires admin token. Optional filters:

- `limit`
- `agentId` / `agent`
- `profileId` / `profile`
- `status`

History records intentionally do not store fill text values, cookies, request headers, screenshot bytes, or full snapshot text.

### `GET /browser-nav/runs/:runId`

Returns one sanitized run-history record by id. Requires admin token.

### `POST /browser-nav/plan`

Validates and normalizes a requested run without opening a browser.

Example:

```json
{
  "agentId": "mark",
  "profileId": "mark",
  "reason": "Check portal landing page",
  "steps": [
    { "action": "navigate", "url": "https://example.com" },
    { "action": "snapshot", "maxChars": 1000 }
  ]
}
```

### `POST /browser-nav/runs`

Runs the same request shape. Behavior:

- with `dryRun: true`: validates/plans only;
- with live disabled: returns `blocked-live-disabled`;
- with live enabled and profile CDP configured: opens an isolated CDP target and executes steps.

## Supported actions

- `navigate`: `{ "action": "navigate", "url": "https://example.com" }`
- `click`: `{ "action": "click", "selector": "button[type=submit]" }`
- `fill`: `{ "action": "fill", "selector": "input[name=email]", "text": "user@example.com" }`
- `press`: `{ "action": "press", "key": "Enter" }`
- `waitForSelector`: `{ "action": "waitForSelector", "selector": ".ready" }`
- `snapshot`: `{ "action": "snapshot", "maxChars": 4000 }`
- `screenshot`: `{ "action": "screenshot", "fullPage": true }`

## Current v1 limitation

This is a CDP-level API, not a state-portal filing judgment engine. It can navigate/click/fill/read pages through configured browser profiles, but filing submission, payment/billing/security changes, CAPTCHA/MFA, Google Workspace browser surfaces, and uncertain filing decisions remain approval/stop points.
