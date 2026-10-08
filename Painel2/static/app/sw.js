/* Service worker do app Painel2 - guarda a "casca" do app para abrir rapido;
   os alertas vem sempre do servidor (rede primeiro). */
const CACHE = 'painel2-app-v1';
const SHELL = ['/app', '/static/theme.css', '/static/app/icon-192.png', '/static/img/logo-gazeta-branco.png',
               '/static/fonts/manrope-latin-wght-normal.woff2'];
self.addEventListener('install', e => { e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).catch(() => {})); self.skipWaiting(); });
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))));
  self.clients.claim();
});
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if(e.request.method !== 'GET' || url.origin !== location.origin) return;
  if(url.pathname.startsWith('/api/')) return;           // dados: sempre do servidor
  e.respondWith(fetch(e.request).then(r => {
    if(r.ok && (SHELL.includes(url.pathname) || url.pathname.startsWith('/static/'))) {
      const copy = r.clone(); caches.open(CACHE).then(c => c.put(e.request, copy));
    }
    return r;
  }).catch(() => caches.match(e.request)));
});
