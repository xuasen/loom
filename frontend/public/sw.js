/* Loom service worker: shows Claude Terminal notifications (Web Push) and
 * opens the session when one is tapped. No caching: the app is always loaded
 * from the network. */
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("push", (event) => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch {
    data = { title: "Loom", body: event.data ? event.data.text() : "" };
  }
  const url = typeof data.url === "string" && data.url.startsWith("/") ? data.url : "/";
  event.waitUntil(
    (async () => {
      // Skip it when the user is already looking at this session.
      const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
      const watching = windows.some(
        (c) => c.focused && c.visibilityState === "visible" && data.session_id && c.url.includes(data.session_id),
      );
      if (watching) return;
      await self.registration.showNotification(data.title || "Loom", {
        body: data.body || "",
        tag: data.tag || undefined,
        renotify: true,
        icon: "/assets/loom_favicon.png",
        badge: "/assets/loom_favicon.png",
        data: { url },
      });
    })(),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = new URL(event.notification.data?.url || "/", self.location.origin).href;
  event.waitUntil(
    (async () => {
      const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
      for (const c of windows) {
        if (new URL(c.url).origin === self.location.origin && "focus" in c) {
          await c.focus();
          if ("navigate" in c) await c.navigate(url);
          return;
        }
      }
      await self.clients.openWindow(url);
    })(),
  );
});
