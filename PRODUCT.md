# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Zenox is a private operations tool for its owner and, when needed, a small internal operator team. Operators use it from a desktop browser and as an installable mobile PWA, including private iPhone access through Tailscale.

## Product Purpose

Zenox coordinates automated chat workflows across multiple platforms. It runs and monitors bots, extracts conversations, generates replies through Chameleon AI, and gives operators a safe place to review, edit, approve, reject, regenerate, or automatically send those replies. Success means urgent approvals are obvious and fast, bot failures are visible before they cause missed conversations, and automation policy remains understandable and controllable.

## Positioning

Zenox combines a live multi-platform bot control plane with a human approval queue, per-platform automation policy, provider health, durable approval history, mobile notifications, and recovery controls in one private operator console.

## Operating Context

Operators keep the dashboard running alongside supervised browser sessions and chat platforms. The daily priority order is urgent reply approvals first, bot health and controls second, and configuration third. The interface must work on desktop for detailed operations and on an iPhone PWA for rapid approvals and incident response. Tailscale provides private remote access rather than exposing the application publicly.

## Capabilities and Constraints

- Preserve every existing route, API contract, form, control, data flow, platform workflow, and business rule.
- Preserve manual and automatic review modes, Judge policies, First Contact handling, message timing, runtime visibility, bot lifecycle controls, credential management, API-key health checks, prompt management, English review translations, earnings checks, alerts, push notifications, and service-worker/PWA behavior.
- Preserve the supported platform set and platform-specific language behavior.
- Approval actions and destructive bot controls must remain unambiguous and safe.
- The application is a server-rendered Flask interface with substantial inline HTML, CSS, and JavaScript in `approval_server.py` plus templates under `templates/`.
- Credentials and provider keys remain server-side and masked in the browser.

## Brand Commitments

Preserve the Zenox name and existing Zenox logo assets. Preserve a dark-first product experience. The replacement visual system must feel specific to an always-on operations console rather than a generic admin dashboard, while remaining restrained and highly legible during long sessions.

## Evidence on Hand

The repository contains real operational copy, live status and lifecycle data, persisted approval history, platform health information, PWA assets, and production workflows. It does not contain public marketing claims, customer testimonials, benchmarks, or commercial proof; future work must not invent them.

## Product Principles

1. Put the next operator decision ahead of ambient status.
2. Make automation state, ownership, and recovery paths visible at a glance.
3. Keep high-frequency actions fast while making risky actions unmistakable.
4. Preserve product truth and operational reliability through every visual change.
5. Treat mobile approval and desktop supervision as equally real operating contexts.

## Accessibility & Inclusion

Meet WCAG 2.1 AA expectations across desktop and mobile, including contrast, visible focus, keyboard operation, semantic structure, accessible labels, adequate touch targets, reduced-motion behavior, and validation feedback that is not conveyed by color alone.
