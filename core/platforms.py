"""Canonical platform metadata shared by the launcher and dashboard.

Keep presentation metadata and lifecycle capabilities here so adding a new
platform does not require editing several unrelated pages and control paths.
Credentials and selectors remain in ``configs/<slug>.py``.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Platform:
    slug: str
    label: str
    color: str
    self_managed: bool = False
    enabled_by_default: bool = True


PLATFORMS = (
    Platform("gold", "Gold", "#f59e0b"),
    Platform("gold2", "Gold2", "#eab308", enabled_by_default=False),
    Platform("gold3", "Gold3", "#d946ef", enabled_by_default=False),
    Platform("diamond", "Diamond", "#22d3ee"),
    Platform("platin", "Platin", "#94a3b8"),
    Platform("s69", "S69", "#ec4899"),
    Platform("ml", "ML", "#22c55e"),
    Platform("xkuss", "Xkuss", "#ef4444", self_managed=True),
    Platform("justlo", "Justlo", "#3b82f6", self_managed=True),
    Platform("linduu", "Linduu", "#10b981", self_managed=True),
    Platform("gnoxx", "Gnoxx", "#0ea5e9", self_managed=True),
)

PLATFORM_BY_SLUG = {platform.slug: platform for platform in PLATFORMS}
PLATFORM_BY_LABEL = {platform.label: platform for platform in PLATFORMS}

KNOWN_PLATFORM_SLUGS = tuple(platform.slug for platform in PLATFORMS)
KNOWN_PLATFORM_LABELS = tuple(platform.label for platform in PLATFORMS)
REACT_PLATFORM_SLUGS = tuple(platform.slug for platform in PLATFORMS if not platform.self_managed)
SELF_MANAGED_PLATFORM_SLUGS = tuple(platform.slug for platform in PLATFORMS if platform.self_managed)
DEFAULT_PLATFORM_SLUGS = tuple(platform.slug for platform in PLATFORMS if platform.enabled_by_default)
PLATFORM_COLORS = {platform.label: platform.color for platform in PLATFORMS}


def platform_label(slug: str) -> str:
    """Return a stable display label for a validated platform slug."""
    return PLATFORM_BY_SLUG[slug].label
