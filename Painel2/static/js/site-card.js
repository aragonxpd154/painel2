/* ==========================================================================
   Cartao do local (canto superior direito de cada abrigo)
   - tempo agora + previsao dos proximos dias (Open-Meteo via servidor)
   - mini mapa com a posicao do abrigo (abre em tela cheia p/ ajustar)
   - codigo do cliente EDP para abrir chamado
   ========================================================================== */
(function(){
  const ICON = {
    sun: '<svg viewBox="0 0 64 64"><circle cx="32" cy="32" r="12" fill="#f8c537"/><g stroke="#f8c537" stroke-width="4" stroke-linecap="round"><path d="M32 6v8M32 50v8M6 32h8M50 32h8M13.6 13.6l5.7 5.7M44.7 44.7l5.7 5.7M13.6 50.4l5.7-5.7M44.7 19.3l5.7-5.7"/></g></svg>',
    moon: '<svg viewBox="0 0 64 64"><path d="M40 10a22 22 0 1 0 14 38A24 24 0 0 1 40 10z" fill="#d9e2f5"/></svg>',
    cloud: '<svg viewBox="0 0 64 64"><path d="M20 48h26a11 11 0 0 0 1-22 15 15 0 0 0-29 3 10 10 0 0 0 2 19z" fill="#c9d3e6"/></svg>',
    partly: '<svg viewBox="0 0 64 64"><circle cx="24" cy="22" r="10" fill="#f8c537"/><g stroke="#f8c537" stroke-width="3.5" stroke-linecap="round"><path d="M24 4v5M8 22H3M11 9l3.5 3.5M37 9l-3.5 3.5M9.5 35l3.5-3.5"/></g><path d="M24 52h24a10 10 0 0 0 1-20 13 13 0 0 0-25 3 9 9 0 0 0 0 17z" fill="#dfe6f3"/></svg>',
    partlyNight: '<svg viewBox="0 0 64 64"><path d="M28 6a14 14 0 1 0 12 22A16 16 0 0 1 28 6z" fill="#d9e2f5"/><path d="M24 52h24a10 10 0 0 0 1-20 13 13 0 0 0-25 3 9 9 0 0 0 0 17z" fill="#b8c3dc"/></svg>',
    fog: '<svg viewBox="0 0 64 64"><path d="M18 34h28a10 10 0 0 0 0-20 13 13 0 0 0-25 2 9 9 0 0 0-3 18z" fill="#aab6cc"/><g stroke="#8b95ab" stroke-width="4" stroke-linecap="round"><path d="M12 44h40M18 52h30"/></g></svg>',
    drizzle: '<svg viewBox="0 0 64 64"><path d="M18 38h28a10 10 0 0 0 1-20 13 13 0 0 0-25 3 9 9 0 0 0-4 17z" fill="#b8c3dc"/><g stroke="#5fb4ff" stroke-width="3" stroke-linecap="round"><path d="M24 46l-2 5M34 46l-2 5M44 46l-2 5"/></g></svg>',
    rain: '<svg viewBox="0 0 64 64"><path d="M18 36h28a10 10 0 0 0 1-20 13 13 0 0 0-25 3 9 9 0 0 0-4 17z" fill="#9aa8c2"/><g stroke="#3d9bff" stroke-width="3.5" stroke-linecap="round"><path d="M22 43l-4 10M32 43l-4 10M42 43l-4 10"/></g></svg>',
    storm: '<svg viewBox="0 0 64 64"><path d="M18 34h28a10 10 0 0 0 1-20 13 13 0 0 0-25 3 9 9 0 0 0-4 17z" fill="#7f8ba6"/><path d="M34 36l-8 13h7l-4 11 12-16h-7l5-8z" fill="#f8c537"/></svg>',
  };

  // codigos WMO (Open-Meteo) -> texto em portugues + icone
  function wmo(code, isDay){
    const c = Number(code), day = isDay !== 0;
    if(c === 0) return {t: day ? 'Ensolarado' : 'Céu limpo', i: day ? 'sun' : 'moon'};
    if(c === 1) return {t: day ? 'Predomínio de sol' : 'Poucas nuvens', i: day ? 'partly' : 'partlyNight'};
    if(c === 2) return {t: 'Parcialmente nublado', i: day ? 'partly' : 'partlyNight'};
    if(c === 3) return {t: 'Nublado', i: 'cloud'};
    if(c === 45 || c === 48) return {t: 'Neblina', i: 'fog'};
    if(c >= 51 && c <= 57) return {t: 'Garoa', i: 'drizzle'};
    if(c === 61) return {t: 'Chuva fraca', i: 'drizzle'};
    if(c === 63 || c === 66) return {t: 'Chuva', i: 'rain'};
    if(c === 65 || c === 67) return {t: 'Chuva forte', i: 'rain'};
    if(c >= 71 && c <= 77) return {t: 'Neve', i: 'cloud'};
    if(c === 80) return {t: 'Pancadas de chuva', i: 'drizzle'};
    if(c === 81 || c === 82) return {t: 'Pancadas fortes', i: 'rain'};
    if(c >= 95) return {t: 'Trovoadas', i: 'storm'};
    return {t: '—', i: 'cloud'};
  }
  const fmt = (n, d) => n == null || isNaN(n) ? '—' : Number(n).toLocaleString('pt-BR', {minimumFractionDigits: d || 0, maximumFractionDigits: d || 0});
  const esc = t => { const d = document.createElement('div'); d.textContent = t == null ? '' : String(t); return d.innerHTML; };

  const CSS = `
  .sc{display:flex;flex-direction:column;gap:10px;width:100%;height:100%;text-align:left;}
  .sc-top{display:flex;align-items:center;gap:12px;}
  .sc-top img{height:26px;width:auto;}
  .sc-top .cap{font-size:12px;color:#b8c3dc;line-height:1.25;} .sc-top .cap b{color:#fff;display:block;font-size:13px;}
  .sc-now{display:flex;align-items:center;gap:12px;}
  .sc-ico{width:58px;height:58px;flex-shrink:0;} .sc-ico svg{width:100%;height:100%;}
  .sc-temp{font-size:36px;font-weight:800;line-height:1;color:#fff;}
  .sc-desc{font-size:13px;font-weight:700;color:#dbe3f5;margin-top:3px;}
  .sc-extra{font-size:11px;color:#8b95ab;margin-top:2px;}
  .sc-days{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;}
  .sc-day{background:rgba(7,12,24,0.5);border:1px solid #223050;border-radius:9px;padding:5px 4px;text-align:center;}
  .sc-day .d{font-size:10.5px;color:#8b95ab;font-weight:700;text-transform:capitalize;}
  .sc-day svg{width:30px;height:30px;display:block;margin:2px auto;}
  .sc-day .mx{font-size:12.5px;font-weight:800;color:#fff;} .sc-day .mn{font-size:11px;color:#8b95ab;margin-left:3px;}
  .sc-day .pp{font-size:10px;color:#5fb4ff;}
  .sc-mapbox{position:relative;flex:1;min-height:110px;border-radius:10px;overflow:hidden;border:1px solid #223050;cursor:pointer;}
  .sc-map{position:absolute;inset:0;background:#0b1326;}
  .sc-mapbox .sc-open{position:absolute;right:6px;bottom:6px;z-index:500;font:700 11px Manrope,sans-serif;background:rgba(7,12,24,0.85);color:#fff;
    border:1px solid #2b3856;border-radius:7px;padding:4px 9px;cursor:pointer;}
  .sc-mapbox .sc-aprox{position:absolute;left:6px;top:6px;z-index:500;font:700 10px Manrope,sans-serif;background:rgba(245,196,81,0.92);color:#2a1c00;border-radius:6px;padding:3px 7px;}
  .sc-edp{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:11.5px;color:#b8c3dc;background:rgba(7,12,24,0.5);border:1px solid #223050;border-radius:9px;padding:6px 10px;}
  .sc-edp b{color:#fff;font-size:13px;letter-spacing:.02em;}
  .sc-edp button{font:700 10.5px Manrope,sans-serif;background:#1b2740;color:#cfd8ea;border:1px solid #2b3856;border-radius:6px;padding:3px 8px;cursor:pointer;}
  .sc-msg{font-size:11px;color:#8b95ab;}
  .sc-pin{width:22px;height:22px;border-radius:50% 50% 50% 0;background:#e5384b;transform:rotate(-45deg);border:3px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,.5);}
  .scm{position:fixed;inset:0;z-index:300;background:rgba(4,8,18,0.75);display:none;align-items:center;justify-content:center;}
  .scm.open{display:flex;}
  .scm-card{width:min(1200px,94vw);height:min(820px,90vh);background:#0e1628;border:1px solid #2b3856;border-radius:14px;display:flex;flex-direction:column;overflow:hidden;}
  .scm-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:10px 14px;border-bottom:1px solid #223050;font:13px Manrope,sans-serif;color:#dbe3f5;}
  .scm-bar b{font-size:15px;color:#fff;margin-right:auto;}
  .scm-bar input{width:120px;background:#121d36;color:#fff;border:1px solid #2b3856;border-radius:7px;padding:6px 8px;font:inherit;}
  .scm-bar button,.scm-bar a{font:700 12px Manrope,sans-serif;background:#1b2740;color:#cfd8ea;border:1px solid #2b3856;border-radius:7px;padding:6px 11px;cursor:pointer;text-decoration:none;}
  .scm-bar button.pri{background:#0a4bb5;border-color:#0a4bb5;color:#fff;}
  .scm-bar button.on{border-color:#f5c451;color:#f5c451;}
  .scm-map{flex:1;}
  .scm-hint{font-size:11.5px;color:#f5c451;padding:6px 14px;display:none;}
  `;
  let cssDone = false;
  function addCss(){ if(cssDone) return; cssDone = true; const st = document.createElement('style'); st.textContent = CSS; document.head.appendChild(st); }

  const LAYERS = {
    mapa: () => L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19, attribution: '© OpenStreetMap'}),
    satelite: () => L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
                               {maxZoom: 19, attribution: 'Imagens © Esri'}),
  };
  const pinIcon = () => L.divIcon({className: '', html: '<div class="sc-pin"></div>', iconSize: [22, 22], iconAnchor: [11, 22]});

  function mount(host, local, caption){
    addCss();
    if(!host || !local) return;
    host.innerHTML = `
      <div class="sc">
        <div class="sc-top"><img src="/static/img/logo-gazeta-branco.png" alt="Rede Gazeta"><div class="cap"><b>Transmissão</b>${esc(caption || local.nome || '')}</div></div>
        <div class="sc-now"><div class="sc-ico"></div><div><div class="sc-temp">--°</div><div class="sc-desc">carregando previsão…</div><div class="sc-extra"></div></div></div>
        <div class="sc-days"></div>
        <div class="sc-mapbox"><div class="sc-map"></div>${local.coord_aprox ? '<span class="sc-aprox">posição aproximada</span>' : ''}<button class="sc-open">&#128205; Ver / ajustar no mapa</button></div>
        <div class="sc-edp">&#9889; EDP · código do cliente <b>${esc(local.edp || '—')}</b>
          ${local.edp ? '<button class="sc-copy">copiar</button>' : ''}<span>· ${esc(local.edp_tel || '')}</span></div>
      </div>`;
    // mini mapa (so exibe; o mapa interativo abre em tela cheia - o quadro do
    // abrigo e redimensionado por CSS e o Leaflet nao lida bem com isso)
    const lat = Number(local.lat), lon = Number(local.lon);
    let mini = null;
    if(window.L && !isNaN(lat) && !isNaN(lon)){
      mini = L.map(host.querySelector('.sc-map'), {zoomControl: false, attributionControl: false, dragging: false,
        scrollWheelZoom: false, doubleClickZoom: false, boxZoom: false, keyboard: false, touchZoom: false}).setView([lat, lon], 14);
      LAYERS.satelite().addTo(mini);
      mini._pin = L.marker([lat, lon], {icon: pinIcon(), interactive: false}).addTo(mini);
      setTimeout(() => mini.invalidateSize(), 400);
    }
    host.querySelector('.sc-mapbox').addEventListener('click', () => openMap(local, (nlat, nlon) => {
      local.lat = nlat; local.lon = nlon; local.coord_aprox = false;
      const ap = host.querySelector('.sc-aprox'); if(ap) ap.remove();
      if(mini){ mini.setView([nlat, nlon], 14); mini._pin.setLatLng([nlat, nlon]); }
      loadWeather();
    }));
    const cp = host.querySelector('.sc-copy');
    if(cp) cp.addEventListener('click', e => { e.stopPropagation();
      navigator.clipboard && navigator.clipboard.writeText(local.edp).then(() => { cp.textContent = 'copiado ✓'; setTimeout(() => cp.textContent = 'copiar', 1500); }); });

    async function loadWeather(){
      try{
        const d = await (await fetch('/api/weather?slug=' + encodeURIComponent(local.slug), {cache: 'no-store'})).json();
        if(d.error && !d.current){ host.querySelector('.sc-desc').textContent = 'previsão indisponível'; host.querySelector('.sc-extra').textContent = d.error; return; }
        const c = d.current || {}; const w = wmo(c.weather_code, c.is_day);
        host.querySelector('.sc-ico').innerHTML = ICON[w.i];
        host.querySelector('.sc-temp').textContent = fmt(c.temperature_2m, 0) + '°C';
        host.querySelector('.sc-desc').textContent = w.t;
        host.querySelector('.sc-extra').textContent = 'sensação ' + fmt(c.apparent_temperature, 0) + '° · umidade ' + fmt(c.relative_humidity_2m, 0) +
          '% · vento ' + fmt(c.wind_speed_10m, 0) + ' km/h' + (d.stale ? ' · (sem atualizar)' : '');
        const dl = d.daily || {}; const days = (dl.time || []).slice(1, 6);
        host.querySelector('.sc-days').innerHTML = days.map((t, k) => {
          const i = k + 1, ww = wmo(dl.weather_code[i], 1);
          const nm = new Date(t + 'T12:00:00').toLocaleDateString('pt-BR', {weekday: 'short'}).replace('.', '');
          const pp = dl.precipitation_probability_max ? dl.precipitation_probability_max[i] : null;
          return '<div class="sc-day" title="' + ww.t + '"><div class="d">' + nm + '</div>' + ICON[ww.i] +
            '<span class="mx">' + fmt(dl.temperature_2m_max[i], 0) + '°</span><span class="mn">' + fmt(dl.temperature_2m_min[i], 0) + '°</span>' +
            (pp != null ? '<div class="pp">' + (pp > 0 ? '&#128167; ' + pp + '%' : '&nbsp;') + '</div>' : '') + '</div>';
        }).join('');
      }catch(e){ host.querySelector('.sc-desc').textContent = 'previsão indisponível'; }
    }
    loadWeather();
    setInterval(loadWeather, 10 * 60 * 1000);
  }

  // ---------------- mapa em tela cheia (ver / ajustar posicao) ----------------
  let modal = null, big = null, bigPin = null, layerNow = 'satelite', baseLayer = null, editing = false, cur = null, onSaved = null;
  function openMap(local, saved){
    addCss(); cur = local; onSaved = saved;
    if(!modal){
      modal = document.createElement('div'); modal.className = 'scm';
      modal.innerHTML = `<div class="scm-card">
        <div class="scm-bar"><b class="scm-title"></b>
          lat <input class="scm-lat"> lon <input class="scm-lon">
          <button class="scm-layer">Mapa de ruas</button>
          <button class="scm-edit">&#128205; Ajustar posição</button>
          <button class="scm-save pri">Salvar posição</button>
          <a class="scm-gmaps" target="_blank" rel="noopener">Google Maps &#8599;</a>
          <button class="scm-close">&#10005;</button></div>
        <div class="scm-hint">Arraste o marcador ou clique no ponto exato da torre/abrigo. Depois clique em "Salvar posição".</div>
        <div class="scm-map"></div></div>`;
      document.body.appendChild(modal);
      modal.addEventListener('click', e => { if(e.target === modal) close(); });
      modal.querySelector('.scm-close').onclick = close;
      document.addEventListener('keydown', e => { if(e.key === 'Escape' && modal.classList.contains('open')) close(); });
      modal.querySelector('.scm-layer').onclick = () => { layerNow = layerNow === 'satelite' ? 'mapa' : 'satelite'; setLayer(); };
      modal.querySelector('.scm-edit').onclick = () => setEdit(!editing);
      modal.querySelector('.scm-save').onclick = save;
      ['.scm-lat', '.scm-lon'].forEach(s => modal.querySelector(s).addEventListener('change', () => {
        const la = parseFloat(modal.querySelector('.scm-lat').value.replace(',', '.')), lo = parseFloat(modal.querySelector('.scm-lon').value.replace(',', '.'));
        if(!isNaN(la) && !isNaN(lo)){ bigPin.setLatLng([la, lo]); big.setView([la, lo]); updLink(); }
      }));
    }
    modal.querySelector('.scm-title').textContent = (local.nome || 'Abrigo') + (local.coord_aprox ? ' — posição aproximada' : '');
    modal.classList.add('open');
    const lat = Number(local.lat), lon = Number(local.lon);
    if(!big){
      big = L.map(modal.querySelector('.scm-map')).setView([lat, lon], 16);
      bigPin = L.marker([lat, lon], {icon: pinIcon(), draggable: false}).addTo(big);
      bigPin.on('drag', updFields); bigPin.on('dragend', updFields);
      big.on('click', e => { if(editing){ bigPin.setLatLng(e.latlng); updFields(); } });
      setLayer();
    } else {
      big.setView([lat, lon], 16); bigPin.setLatLng([lat, lon]);
    }
    setTimeout(() => big.invalidateSize(), 200);
    setEdit(false); updFields();
  }
  function setLayer(){
    if(baseLayer) big.removeLayer(baseLayer);
    baseLayer = LAYERS[layerNow]().addTo(big);
    modal.querySelector('.scm-layer').textContent = layerNow === 'satelite' ? 'Mapa de ruas' : 'Satélite';
  }
  function setEdit(on){
    editing = on;
    if(bigPin.dragging) on ? bigPin.dragging.enable() : bigPin.dragging.disable();
    modal.querySelector('.scm-edit').classList.toggle('on', on);
    modal.querySelector('.scm-hint').style.display = on ? 'block' : 'none';
    modal.querySelector('.scm-lat').disabled = !on; modal.querySelector('.scm-lon').disabled = !on;
  }
  function updLink(){
    const p = bigPin.getLatLng();
    modal.querySelector('.scm-gmaps').href = 'https://www.google.com/maps?q=' + p.lat.toFixed(6) + ',' + p.lng.toFixed(6);
  }
  function updFields(){
    const p = bigPin.getLatLng();
    modal.querySelector('.scm-lat').value = p.lat.toFixed(6);
    modal.querySelector('.scm-lon').value = p.lng.toFixed(6);
    updLink();
  }
  async function save(){
    const p = bigPin.getLatLng();
    try{
      const r = await fetch('/api/abrigos/location', {method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({slug: cur.slug, lat: p.lat, lon: p.lng})});
      const d = await r.json(); if(!d.ok) throw new Error(d.error);
      setEdit(false);
      modal.querySelector('.scm-title').textContent = (cur.nome || 'Abrigo') + ' — posição salva ✓';
      if(onSaved) onSaved(p.lat, p.lng);
    }catch(e){ alert('Não foi possível salvar: ' + e.message); }
  }
  function close(){ modal.classList.remove('open'); setEdit(false); }

  window.SiteCard = {mount, openMap, wmo, ICON};
})();
