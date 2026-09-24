const CACHE_NAME = "zenox-pwa-v2";
const APP_ASSETS = [
  "/",
  "/static/manifest.webmanifest",
  "/static/icons/zenox-192-v2.png",
  "/static/icons/zenox-512-v2.png"
];

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(APP_ASSETS))
      .catch(() => undefined)
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("push", event => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch (_) {
    data = { body: event.data ? event.data.text() : "A reply needs approval." };
  }

  const pendingCount = Number(data.pendingCount || 0);
  const notify = self.registration.showNotification(data.title || "Zenox approval", {
    body: data.body || "A generated reply is waiting for your decision.",
    icon: data.icon || "/static/icons/zenox-192-v2.png",
    badge: data.badge || "/static/icons/zenox-192-v2.png",
    tag: data.tag || "zenox-approval",
    renotify: true,
    data: { url: data.url || "/", requestId: data.requestId || null }
  });

  const badge = pendingCount > 0 && self.registration.setAppBadge
    ? self.registration.setAppBadge(pendingCount)
    : (pendingCount === 0 && self.registration.clearAppBadge
      ? self.registration.clearAppBadge()
      : Promise.resolve());
  event.waitUntil(Promise.all([notify, badge]));
});

self.addEventListener("notificationclick", event => {
  event.notification.close();
  const targetUrl = new URL(event.notification.data?.url || "/", self.location.origin).href;
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then(windows => {
      for (const client of windows) {
        if ("navigate" in client) client.navigate(targetUrl);
        if ("focus" in client) return client.focus();
      }
      return self.clients.openWindow(targetUrl);
    })
  );
});

self.addEventListener("pushsubscriptionchange", event => {
  event.waitUntil(
    self.registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: event.oldSubscription?.options?.applicationServerKey
    }).then(subscription => fetch("/api/push/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ subscription: subscription.toJSON(), send_test: false })
    }))
  );
});
