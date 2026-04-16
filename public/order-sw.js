// Spicetopia Order Portal — Service Worker
// Strategy: cache-first for app shell, network-first for API
// Products are cached so the list loads without signal

const CACHE  = 'sp-order-v1';
const SHELL  = ['/order.html', '/order-manifest.json'];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // API calls — network only (order submission needs live connection)
  // Exception: /api/field/products — cache for offline product list
  if (url.pathname.startsWith('/api/')) {
    if (url.pathname === '/api/field/products') {
      e.respondWith(
        fetch(e.request)
          .then(r => {
            const clone = r.clone();
            caches.open(CACHE).then(c => c.put(e.request, clone));
            return r;
          })
          .catch(() => caches.match(e.request))
      );
      return;
    }
    // All other API calls — network only, no fallback
    return;
  }

  // App shell — cache first
  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request))
  );
});
