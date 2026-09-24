# Zenox chat bots

Playwright bots that read incoming platform chats, copy the conversation HTML
into Chameleon AI, generate a reply, and wait for approval before sending.

## Start the project

Install dependencies once:

```powershell
python -m pip install -r requirements.txt
```

Start the normal supervised workflow:

```powershell
python launch_all.py
```

The launcher starts the approval dashboard automatically. On this PC, open:

```text
http://127.0.0.1:8799/
```

Open `/bots` to start or stop any supported platform individually. Gold,
Gold2, Gold3, Diamond, Platin, S69, ML, Xkuss, Justlo, Linduu, and Gnoxx all
use the same lifecycle controls. If only the dashboard is running, starting
any platform also starts the central launcher automatically.

The dashboard is the control center: Pause/Resume All and the Chameleon Fix,
Check, and Extractor actions keep working even if the launcher's local control
port is temporarily unavailable. Bots started from `/bots` use the central
supervisor so Restart and the full control set remain available afterward.

Open `/api-keys` (or choose **API Keys** in the sidebar) to replace provider
keys without restarting the dashboard. Saved keys are written to the ignored
`.env` file and take effect immediately; the page returns only masked key
suffixes to the browser. **Check all** performs a small live request against
each configured provider.

Open `/settings` (or choose **Settings** in the sidebar) for policies shared
by every platform. Manual mode can show or skip Judge analysis while still
requiring human approval. Automatic mode can use the current 10/10 + manual
fallback, reject lower Judge scores so the bot regenerates, or send directly
without Judge. Justlo, Linduu, and Gnoxx can either transfer/skip Chameleon
First Contact conversations or answer them through the normal workflow.
Choices are saved locally in the ignored `.runtime_settings.json` file and
are read for each new conversation. When the file does not exist, the current
production behavior remains unchanged.

To run only the dashboard, double-click `launch_dashboard.bat`.

## Private phone access with Tailscale

The application listens only on `127.0.0.1:8799`. Tailscale Serve provides a
private HTTPS proxy, so port 8799 is not exposed to the Wi-Fi network or public
internet.

One-time setup:

1. Keep Tailscale installed and signed in on this PC.
2. Install Tailscale on the phone and sign in with the same Tailscale account.
3. Double-click `setup_tailscale.bat` and accept the Windows administrator
   prompt. If Tailscale prints a web link asking to enable Serve/HTTPS, open it,
   approve it once, and run the setup file again.
4. Start the project with `python launch_all.py`.
5. On the phone, connect Tailscale and open the value of
   `TAILSCALE_DASHBOARD_URL` from `.env`.

Configured phone URL:

```text
https://desktop-3bikok9.tailbe422e.ts.net/
```

For remote access to work, this PC must stay awake and connected, Tailscale
must show Connected on both devices, and `launch_all.py` (or
`launch_dashboard.bat`) must still be running.

## iPhone approval notifications

Zenox is an installable Home Screen web app. A new manual approval sends a
background notification containing the platform and latest message. The app
icon shows the pending count, and tapping a notification opens the matching
approval card.

One-time iPhone setup:

1. Open the Tailscale dashboard URL in **Safari**.
2. Tap **Share** (the square with the up arrow).
3. Tap **Add to Home Screen**, keep the name `Zenox`, then tap **Add**.
4. Leave Safari and open the new Zenox icon from the Home Screen.
5. Tap **Enable phone notifications** in the Zenox sidebar.
6. Tap **Allow** on the iPhone permission dialog.
7. Wait for the `Zenox notifications enabled` test notification.

Turning **Sound** on plays the approval chime immediately. Turning
**Notifications** on automatically sends a test push to the newly subscribed
device, so separate test buttons are not needed.

The subscription is saved only on this PC in `.push_subscriptions.json`. The
VAPID signing key is saved in the ignored `secrets` folder. Do not delete or
regenerate that key while the phone is subscribed; changing it requires
disabling and enabling notifications again on the phone.

If notifications are blocked, open iPhone **Settings → Notifications → Zenox**
and enable **Allow Notifications**, Lock Screen, Notification Center, Banners,
Sounds, and Badges.

This project uses **Tailscale Serve**, not Funnel. Serve keeps the dashboard
private to authenticated devices in the tailnet. Do not replace it with Funnel
unless you intentionally want a public internet endpoint.

## Network settings

The local `.env` contains all runtime settings and credentials and is ignored
by Git. `.env.example` documents every required variable without secrets.

| Variable | Purpose | Default |
| --- | --- | --- |
| `HOST` | Dashboard bind address | `127.0.0.1` |
| `APPROVAL_SERVER_PORT` | Dashboard port | `8799` |
| `APPROVAL_SERVER_URL` | URL bots use locally | `http://127.0.0.1:8799` |
| `CONTROL_SERVER_PORT` | Local launcher control API | `8800` |
| `LAUNCHER_CONTROL_URL` | URL dashboard uses for launcher control | `http://127.0.0.1:8800` |
| `TAILSCALE_SERVE_TARGET` | Local service proxied by Tailscale | `http://127.0.0.1:8799` |
| `TAILSCALE_DASHBOARD_URL` | Private phone URL | Tailscale HTTPS name |
| `APPROVAL_USER` / `APPROVAL_PASS` | Optional additional Basic Auth | blank |
| `VAPID_PRIVATE_KEY` | Local Web Push signing key | `secrets/private_key.pem` |
| `VAPID_PUBLIC_KEY` | Public application-server key used by the phone | generated locally |
| `VAPID_SUBJECT` | Contact identity included in push signatures | `mailto:` address |
| `PUSH_SUBSCRIPTIONS_FILE` | Local saved phone subscriptions | `.push_subscriptions.json` |
| `OPENROUTER_API_KEY` | Judge primary provider | blank |
| `NVIDIA_API_KEY` | Judge fallback 1 | blank |
| `GEMINI_API_KEY` | Judge fallback 2 | blank |
| `BAI_API_KEY` | Judge fallback 3 | blank |

Keep `HOST`, `APPROVAL_SERVER_URL`, and `LAUNCHER_CONTROL_URL` local. The phone
uses only `TAILSCALE_DASHBOARD_URL`.

## Approval workflow

Each approval card shows the platform, extracted fake-account/customer data,
the latest customer message, and the proposed response. The response can be
edited before approval.

- **Approve & Send** sends the text currently in the card.
- **Reject & Regenerate** asks Chameleon to generate another response.
- The Approval Queue shows only platforms whose bot process is currently
  running. Start stopped platforms from the **Bots** page.
- **Stop** cancels approvals that the stopped bot can no longer complete, so
  orphaned cards do not remain in the queue.

If the dashboard is unavailable, bots wait instead of sending without review.

The Judge tries OpenRouter, NVIDIA, Gemini, then B.AI. Network, authentication,
rate-limit, malformed-response, and model errors move to the next provider.
If all four fail, the reply stays available for manual review instead of being
sent automatically.

Dashboard state is delivered over WebSocket while the connection is healthy.
HTTP polling runs only as a fallback, and slow launcher-status checks run in
the background so they do not block queue rendering.

## Useful checks

Check the local dashboard:

```powershell
Invoke-WebRequest http://127.0.0.1:8799/api/health -UseBasicParsing
```

Check Tailscale and Serve from an Administrator PowerShell:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" status
& "C:\Program Files\Tailscale\tailscale.exe" serve status
```

If remote access stops working, first verify the project is running locally,
then verify Tailscale is Connected on the PC and phone.
