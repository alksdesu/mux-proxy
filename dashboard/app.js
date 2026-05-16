const API = window.location.origin;
let KEY = '';
let activeTab = 'overview';
const CHANNEL_STORAGE_KEY = 'mux-proxy-channel';
let currentChannel = sessionStorage.getItem(CHANNEL_STORAGE_KEY) || 'all';
let refreshTimer = null;
let refreshMs = 10000;
let refreshRunning = false;
let refreshEnabled = false;
let mapInstance = null;
let mapMarkers = [];
let usageData = [];

// chart instances
let chartRequests = null, chartCost = null, chartModels = null, chartKeys = null;

// 从 :root 读 CSS var：设计系统颜色单一来源，避免 hex 散落在 JS。
const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const chartColors = ['--chart-1','--chart-2','--chart-3','--chart-4','--chart-5','--chart-6','--chart-7','--chart-8'].map(cssVar);
const chartDefaults = {
  color: cssVar('--chart-axis'),
  borderColor: cssVar('--chart-grid'),
};
const chartReqLine = cssVar('--chart-req-line');
const chartReqFill = cssVar('--chart-req-fill');
const chartCostLine = cssVar('--chart-cost-line');
const chartCostFill = cssVar('--chart-cost-fill');
const mapMarkerColor = cssVar('--map-marker');

Chart.defaults.color = chartDefaults.color;
Chart.defaults.borderColor = chartDefaults.borderColor;
Chart.defaults.font.family = "'DM Sans', sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.labels.boxWidth = 10;
Chart.defaults.plugins.legend.labels.padding = 12;
Chart.defaults.elements.point.radius = 2;
Chart.defaults.elements.point.hoverRadius = 4;

// ===== CHANNEL =====
function appendChannel(path) {
  if (currentChannel === 'all') return path;
  const sep = path.includes('?') ? '&' : '?';
  return path + sep + 'channel=' + encodeURIComponent(currentChannel);
}

function channelTag(channel) {
  if (!channel) return '';
  const label = channel.charAt(0).toUpperCase() + channel.slice(1);
  return `<span class="channel-tag ch-${channel}">${label}</span>`;
}

// 新增渠道时只需在此扩展。
const CHANNEL_META = {
  copilot:   { placeholder: 'enterprise:ghp_xxx',  label: '上游密钥（enterprise:ghp_xxx）',  testModel: 'claude-opus-4-6' },
  anthropic: { placeholder: 'anthropic:sk-ant-xxx', label: '上游密钥（anthropic:sk-ant-xxx）', testModel: 'claude-sonnet-4-5' },
};

function channelMeta(channel) {
  return CHANNEL_META[channel] || CHANNEL_META.copilot;
}

// 把当前渠道的 placeholder / label / Fast 可见性写入指定 modal 字段。
// fastGroupId 传 null 表示该 modal 没有 Fast 字段（如上游密钥 modal）。
function applyChannelToModal(channel, { keyInputId, keyLabelId, fastGroupId, labelTemplate }) {
  const meta = channelMeta(channel);
  document.getElementById(keyInputId).placeholder = meta.placeholder;
  document.getElementById(keyLabelId).textContent = labelTemplate
    ? labelTemplate.replace('{placeholder}', meta.placeholder)
    : meta.label;
  if (fastGroupId) {
    document.getElementById(fastGroupId).style.display = channel === 'anthropic' ? 'none' : '';
  }
}

function pickChannelKeys(snapshot) {
  if (!snapshot) return [];
  if (currentChannel === 'all') return snapshot.keys || [];
  const grouped = snapshot.keys_by_channel || {};
  return grouped[currentChannel] || [];
}

function applyChannel(ch) {
  if (currentChannel === ch) return;
  currentChannel = ch;
  sessionStorage.setItem(CHANNEL_STORAGE_KEY, ch);
  document.querySelectorAll('.ch-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.channel === ch);
  });
  loadTab(activeTab);
}

document.querySelectorAll('.ch-btn').forEach(btn => {
  if (btn.dataset.channel === currentChannel) btn.classList.add('active');
  else btn.classList.remove('active');
  btn.addEventListener('click', () => applyChannel(btn.dataset.channel));
});

// ===== AUTH =====
function toggleMobileSidebar() {
  const sidebar = document.getElementById('sidebar');
  const backdrop = document.getElementById('sidebar-backdrop');
  const isOpen = sidebar.classList.toggle('open');
  backdrop.classList.toggle('visible', isOpen);
  document.body.style.overflow = isOpen ? 'hidden' : '';
}

function doLogin() {
  const k = document.getElementById('key-input').value.trim();
  if (!k) return;
  KEY = k;
  testAuth().then(ok => {
    if (ok) {
      sessionStorage.setItem('admin_key', KEY);
      showApp();
    } else {
      document.getElementById('login-error').textContent = '密钥无效';
      KEY = '';
    }
  });
}

async function testAuth() {
  try {
    const r = await fetch(`${API}/stats`, { headers: { Authorization: `Bearer ${KEY}` } });
    return r.ok;
  } catch { return false; }
}

function showApp() {
  document.getElementById('login-screen').style.display = 'none';
  document.getElementById('app').style.display = 'block';
  loadPricing();
  const savedTab = localStorage.getItem('activeTab');
  if (savedTab) switchTab(savedTab);
  else switchTab('overview');
  updateConnStatus(true);
  connectWebSocket();
}

function logout() {
  sessionStorage.removeItem('admin_key');
  KEY = '';
  wsAuthFailed = false;
  if (ws) { ws.onclose = null; ws.close(); ws = null; }
  if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }
  stopRefresh();
  location.reload();
}

// ===== API =====
async function api(method, path, body) {
  const opts = { method, headers: { 'Authorization': `Bearer ${KEY}`, 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`${API}${path}`, opts);
  if (!r.ok) {
    const t = await r.text();
    if (r.status === 404 && t.includes('could not be found') && (path.startsWith('/stats') || path.startsWith('/admin/'))) {
      logout(); throw new Error('auth');
    }
    throw new Error(t);
  }
  return r.json();
}

// ===== TABS =====
document.querySelectorAll('.nav-item[data-tab]').forEach(el => {
  el.addEventListener('click', () => switchTab(el.dataset.tab));
});

function switchTab(tab) {
  if (!tab) return;
  activeTab = tab;
  localStorage.setItem('activeTab', tab);
  document.querySelectorAll('.nav-item').forEach(e => e.classList.toggle('active', e.dataset.tab === tab));
  document.querySelectorAll('.tab-panel').forEach(e => e.classList.toggle('active', e.id === `panel-${tab}`));
  // 移动端切换 tab 后自动关闭侧栏
  if (window.innerWidth < 768) {
    const sidebar = document.getElementById('sidebar');
    if (sidebar.classList.contains('open')) toggleMobileSidebar();
  }
  loadTab(tab);
}

function loadTab(tab) {
  switch(tab) {
    case 'overview': loadOverview(); break;
    case 'keys': loadKeys(); break;
    case 'usage': loadUsage(); break;
    case 'errors': loadErrors(); break;
    case 'map': loadMap(); break;
    case 'system': loadSystem(); break;
  }
}

// ===== OVERVIEW =====
async function loadOverview() {
  try {
    const [stats, ts] = await Promise.all([
      api('GET', appendChannel('/stats')),
      api('GET', appendChannel('/admin/stats/timeseries?hours=24')),
    ]);
    renderMetrics(stats);
    renderCharts(stats, ts);
    renderFeed(stats.recent_requests || []);
    updateConnStatus(true);
  } catch(e) {
    if (e.message !== 'auth') updateConnStatus(false);
  }
}

function renderMetrics(s) {
  const fmtUsd = (v) => '$' + Number(v || 0).toFixed(2);
  const b = s.billing || {};
  const cards = [
    { label: '总请求', value: Number(s.total_requests || 0).toLocaleString(), sub: '' },
    { label: '总费用', value: fmtUsd(b.total_cost_usd), sub: `标准 ${fmtUsd(b.standard_cost_usd)} / Fast ${fmtUsd(b.fast_cost_usd)}` },
    { label: '缓存节省', value: fmtUsd(b.cache_saved_usd), sub: `${fmtNum(s.total_cache_read_tokens)} tokens 读取` },
    { label: '活跃 Key', value: s.active_keys ?? 0, sub: '' },
    { label: '输入 Tokens', value: fmtNum(s.total_input_tokens), sub: `+${fmtNum(s.total_cache_creation_tokens)} 缓存写入` },
    { label: '输出 Tokens', value: fmtNum(s.total_output_tokens), sub: '' },
  ];
  document.getElementById('metric-cards').innerHTML = cards.map(c => `
    <div class="metric-card">
      <div class="label">${c.label}</div>
      <div class="value">${c.value}</div>
      ${c.sub ? `<div class="sub">${c.sub}</div>` : ''}
    </div>`).join('');
}

function renderCharts(stats, ts) {
  // aggregate timeseries by bucket
  const bucketMap = {};
  for (const row of ts) {
    const b = row.bucket;
    if (!bucketMap[b]) bucketMap[b] = { requests: 0, cost: 0 };
    bucketMap[b].requests += row.requests;
    bucketMap[b].cost += calcCostClient(row.model, row.input_tokens, row.output_tokens, row.cache_creation_tokens, row.cache_read_tokens, row.channel_kind);
  }
  const buckets = Object.keys(bucketMap).sort();
  const labels = buckets.map(b => fmtTime(b.endsWith('Z') ? b : b + 'Z'));
  const reqData = buckets.map(b => bucketMap[b].requests);
  const costData = buckets.map(b => +bucketMap[b].cost.toFixed(4));

  // requests chart
  if (chartRequests) chartRequests.destroy();
  chartRequests = new Chart(document.getElementById('chart-requests'), {
    type: 'line',
    data: { labels, datasets: [{ label: 'Requests', data: reqData, borderColor: chartReqLine, backgroundColor: chartReqFill, fill: true, tension: 0.3, borderWidth: 1.5 }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: { grid: { display: false } } } }
  });

  // cost chart
  if (chartCost) chartCost.destroy();
  chartCost = new Chart(document.getElementById('chart-cost'), {
    type: 'line',
    data: { labels, datasets: [{ label: 'Cost ($)', data: costData, borderColor: chartCostLine, backgroundColor: chartCostFill, fill: true, tension: 0.3, borderWidth: 1.5 }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: { grid: { display: false } } } }
  });

  // models doughnut
  const byModel = stats.by_model || {};
  const modelNames = Object.keys(byModel);
  const modelReqs = modelNames.map(m => byModel[m]?.requests || 0);
  if (chartModels) chartModels.destroy();
  chartModels = new Chart(document.getElementById('chart-models'), {
    type: 'doughnut',
    data: { labels: modelNames, datasets: [{ data: modelReqs, backgroundColor: chartColors.slice(0, modelNames.length), borderWidth: 0 }] },
    options: { responsive: true, cutout: '65%', plugins: { legend: { position: 'bottom' } } }
  });

  // keys bar
  const byKey = stats.by_key || {};
  const keyNames = Object.keys(byKey);
  const keyCosts = keyNames.map(k => Number(byKey[k]?.cost_usd || 0));
  if (chartKeys) chartKeys.destroy();
  chartKeys = new Chart(document.getElementById('chart-keys'), {
    type: 'bar',
    data: { labels: keyNames, datasets: [{ label: 'Cost ($)', data: keyCosts, backgroundColor: chartColors.slice(0, keyNames.length), borderRadius: 4, borderWidth: 0 }] },
    options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { display: false } } } }
  });
}

function renderFeed(recent) {
  const last10 = recent.slice(-10).reverse();
  document.getElementById('recent-feed').innerHTML = last10.length ? last10.map(r => `
    <div class="feed-item">
      <span class="feed-time">${fmtTime(r.time)}</span>
      <span class="feed-channel">${channelTag(r.channel_kind)}</span>
      <span class="feed-model">${esc(r.model)}</span>
      <span class="feed-key">${esc(r.key_name)}</span>
      <span class="feed-cost">$${Number(r.cost_usd || 0).toFixed(6)}</span>
    </div>`).join('') : '<div class="loading-placeholder">暂无请求</div>';
}

// ===== KEYS =====
let cachedKeys = [];
async function loadKeys() {
  try {
    const keys = await api('GET', appendChannel('/admin/keys/full'));
    renderKeys(keys);
    populateKeyFilters(keys);
  } catch(e) { if (e.message !== 'auth') toast('加载密钥失败', 'error'); }
}

function renderKeys(keys) {
  cachedKeys = keys;
  const tbody = document.getElementById('keys-tbody');
  tbody.innerHTML = keys.map(k => {
    const masked = k.key.slice(0, 8) + '...' + k.key.slice(-4);
    const pct = k.quota > 0 ? Math.min((k.used / k.quota) * 100, 100) : -1;
    const pctClass = pct >= 95 ? 'danger' : pct >= 80 ? 'warn' : '';
    const uKey = k.upstream_key || '*';
    const isDirect = uKey !== '*' && !/^\d+(,\d+)*$/.test(uKey);
    const uDisplay = isDirect ? (uKey.length > 20 ? uKey.slice(0, 15) + '...' + uKey.slice(-4) : uKey) : '全局池';
    return `<tr>
      <td class="mono" data-col="id">${k.id}</td>
      <td data-col="name">${esc(k.name)}</td>
      <td data-col="channel">${channelTag(k.channel_kind)}</td>
      <td data-col="key">
        <span class="key-masked" onclick="toggleKeyReveal(this,'${esc(k.key)}')" title="点击显示/复制">${masked}</span>
        <button class="btn btn-sm" style="margin-left:4px;padding:2px 6px;font-size:10px" onclick="copyText('${esc(k.key)}')">复制</button>
      </td>
      <td data-col="upstream_key">
        ${isDirect
          ? `<span class="key-masked" onclick="toggleKeyReveal(this,'${esc(uKey)}')" title="点击显示/复制">${esc(uDisplay)}</span>
             <button class="btn btn-sm" style="margin-left:4px;padding:2px 6px;font-size:10px" onclick="copyText('${esc(uKey)}')">复制</button>`
          : `<span class="badge badge-blue">全局池</span>`
        }
      </td>
      <td data-col="quota">
        <span class="mono editable" title="双击编辑" ondblclick="startInlineEdit(this,${k.id},'quota',${k.quota})">${k.quota_display}</span>
        ${pct >= 0 ? `<div class="progress-bar"><div class="progress-fill ${pctClass}" style="width:${pct}%"></div></div>` : ''}
      </td>
      <td class="mono" data-col="used">${k.used_display}</td>
      <td data-col="allow_fast">
        ${(k.channel_kind || 'copilot') === 'anthropic'
          ? `<span style="color:var(--text-muted);font-size:11px" title="Anthropic 渠道无 Fast 版本">—</span>`
          : `<span class="badge ${k.allow_fast ? 'badge-green' : 'badge-red'}" style="cursor:pointer" onclick="toggleFast(${k.id},${k.allow_fast})">${k.allow_fast ? 'ON' : 'OFF'}</span>`}
      </td>
      <td data-col="max_concurrency"><span class="mono editable" title="双击编辑" ondblclick="startInlineEdit(this,${k.id},'max_concurrency',${k.max_concurrency})">${k.max_concurrency === -1 ? '∞' : k.max_concurrency}</span></td>
      <td class="mono" data-col="current_concurrency">${k.current_concurrency}</td>
      <td data-col="rpm_limit"><span class="mono editable" title="双击编辑" ondblclick="startInlineEdit(this,${k.id},'rpm_limit',${k.rpm_limit ?? -1})">${(k.rpm_limit ?? -1) === -1 ? '∞' : k.rpm_limit}</span></td>
      <td class="mono" data-col="rpm_current">${k.rpm_current ?? 0}</td>
      <td class="mono" data-col="created_at" style="font-size:11px">${fmtDate(k.created_at)}</td>
      <td data-col="actions" style="white-space:nowrap">
        <button class="btn btn-sm" onclick="openEditKey(${k.id})">编辑</button>
        <button class="btn btn-sm" onclick="openKeyHistory(${k.id})">记录</button>
        <button class="btn btn-sm" onclick="exportKeyUsage(${k.id})">导出</button>
        <button class="btn btn-sm btn-danger" onclick="confirmDeleteKey(${k.id})">删除</button>
      </td>
    </tr>`;
  }).join('');
}

function toggleKeyReveal(el, fullKey) {
  if (el.dataset.revealed === '1') {
    el.textContent = fullKey.slice(0, 8) + '...' + fullKey.slice(-4);
    el.dataset.revealed = '0';
  } else {
    el.textContent = fullKey;
    el.dataset.revealed = '1';
    copyText(fullKey);
  }
}

function startInlineEdit(el, id, field, currentVal) {
  const input = document.createElement('input');
  input.className = 'inline-edit';
  input.type = 'number';
  input.value = currentVal;
  el.replaceWith(input);
  input.focus();
  input.select();
  const save = async () => {
    const val = parseFloat(input.value);
    if (!isNaN(val)) {
      try {
        await api('PATCH', `/admin/keys?id=${id}`, { [field]: val });
        toast(`已更新 ${field}`, 'success');
      } catch(e) { toast('更新失败', 'error'); }
    }
    loadKeys();
  };
  input.addEventListener('blur', save);
  input.addEventListener('keydown', e => { if (e.key === 'Enter') { input.blur(); } if (e.key === 'Escape') { loadKeys(); } });
}

async function toggleFast(id, current) {
  try {
    await api('PATCH', `/admin/keys?id=${id}`, { allow_fast: !current });
    toast(`Fast 已${current ? '关闭' : '开启'}`, 'success');
    loadKeys();
  } catch(e) { toast('更新失败', 'error'); }
}

function openEditKey(id) {
  const k = cachedKeys.find(x => x.id === id);
  if (!k) return;
  const channel = k.channel_kind || 'copilot';
  document.getElementById('ek-id').value = id;
  document.getElementById('ek-channel-badge').innerHTML = channelTag(channel);
  document.getElementById('ek-name').value = k.name;
  document.getElementById('ek-quota').value = k.quota;
  document.getElementById('ek-concurrency').value = k.max_concurrency;
  document.getElementById('ek-rpm').value = k.rpm_limit ?? -1;
  document.getElementById('ek-fast').value = k.allow_fast ? 'true' : 'false';
  applyChannelToModal(channel, {
    keyInputId: 'ek-upstream',
    keyLabelId: 'ek-upstream-label',
    fastGroupId: 'ek-fast-group',
  });
  const upstreamKey = k.upstream_key || '';
  const isDirect = upstreamKey && upstreamKey !== '*' && !/^\d+(,\d+)*$/.test(upstreamKey);
  document.getElementById('ek-upstream-mode').value = isDirect ? 'direct' : 'pool';
  document.getElementById('ek-upstream').value = isDirect ? upstreamKey : '';
  toggleUpstreamInput();
  document.getElementById('edit-key-title').textContent = `编辑 — ${k.name}`;
  openModal('modal-edit-key');
}

function toggleUpstreamInput() {
  const mode = document.getElementById('ek-upstream-mode').value;
  document.getElementById('ek-upstream-direct-group').style.display = mode === 'direct' ? '' : 'none';
}

async function saveEditKey() {
  const id = parseInt(document.getElementById('ek-id').value);
  const channel = (cachedKeys.find(x => x.id === id) || {}).channel_kind || 'copilot';
  const mode = document.getElementById('ek-upstream-mode').value;
  const upstreamVal = mode === 'direct' ? document.getElementById('ek-upstream').value.trim() : '*';
  const body = {
    name: document.getElementById('ek-name').value.trim(),
    quota: parseFloat(document.getElementById('ek-quota').value),
    max_concurrency: parseInt(document.getElementById('ek-concurrency').value),
    rpm_limit: parseInt(document.getElementById('ek-rpm').value),
    upstream_key: upstreamVal,
  };
  // Anthropic 渠道 allow_fast 无意义，不发以免覆盖。
  if (channel !== 'anthropic') {
    body.allow_fast = document.getElementById('ek-fast').value === 'true';
  }
  try {
    await api('PATCH', `/admin/keys?id=${id}`, body);
    toast('已保存', 'success');
    closeModal('modal-edit-key');
    loadKeys();
  } catch(e) { toast('保存失败: ' + e.message, 'error'); }
}

function syncCreateKeyChannel() {
  applyChannelToModal(document.getElementById('ck-channel').value, {
    keyInputId: 'ck-upstream',
    keyLabelId: 'ck-upstream-label',
    fastGroupId: 'ck-fast-group',
  });
}

function showCreateKeyModal() {
  document.getElementById('ck-name').value = '';
  const defaultChannel = currentChannel === 'anthropic' ? 'anthropic' : 'copilot';
  document.getElementById('ck-channel').value = defaultChannel;
  document.getElementById('ck-upstream-mode').value = 'pool';
  document.getElementById('ck-upstream').value = '';
  document.getElementById('ck-upstream-group').style.display = 'none';
  document.getElementById('ck-quota').value = '-1';
  document.getElementById('ck-concurrency').value = '-1';
  document.getElementById('ck-rpm').value = '-1';
  document.getElementById('ck-fast').value = 'true';
  syncCreateKeyChannel();
  openModal('modal-create-key');
}

async function createKey() {
  const name = document.getElementById('ck-name').value.trim();
  const channel = document.getElementById('ck-channel').value;
  const mode = document.getElementById('ck-upstream-mode').value;
  const upstream = mode === 'direct' ? document.getElementById('ck-upstream').value.trim() : '*';
  if (!name) { toast('名称为必填项', 'error'); return; }
  if (mode === 'direct' && !upstream) { toast('直连模式需要填写上游密钥', 'error'); return; }
  const body = {
    name,
    upstream_key: upstream,
    quota: parseFloat(document.getElementById('ck-quota').value),
    max_concurrency: parseInt(document.getElementById('ck-concurrency').value),
    rpm_limit: parseInt(document.getElementById('ck-rpm').value),
    channel_kind: channel,
  };
  if (channel !== 'anthropic') {
    body.allow_fast = document.getElementById('ck-fast').value === 'true';
  }
  try {
    const res = await api('POST', '/admin/keys', body);
    toast(`密钥已创建: ${res.key}`, 'success');
    closeModal('modal-create-key');
    loadKeys();
  } catch(e) { toast('创建失败: ' + e.message, 'error'); }
}



async function testUpstreamKey(upstreamKey, btnEl, channelKind) {
  const resultEl = btnEl?.nextElementSibling;
  if (btnEl) { btnEl.disabled = true; btnEl.textContent = '...'; }
  if (resultEl) { resultEl.className = 'test-result loading'; resultEl.textContent = '测试中'; }
  const model = channelMeta(channelKind || 'copilot').testModel;
  try {
    const r = await fetch(`${API}/v1/messages`, {
      method: 'POST',
      headers: { 'x-api-key': upstreamKey, 'Content-Type': 'application/json', 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model, max_tokens: 5, messages: [{ role: 'user', content: 'hi' }] })
    });
    const ok = r.status === 200;
    if (resultEl) {
      resultEl.className = `test-result ${ok ? 'ok' : 'fail'}`;
      resultEl.textContent = ok ? '✓' : `✗ ${r.status}`;
    }
    return ok;
  } catch(e) {
    if (resultEl) { resultEl.className = 'test-result fail'; resultEl.textContent = '✗ 网络错误'; }
    return false;
  } finally {
    if (btnEl) { btnEl.disabled = false; btnEl.textContent = '测试'; }
  }
}

async function testAllKeys() {
  const btn = document.getElementById('btn-test-all');
  if (!cachedKeys.length) { toast('没有密钥', 'error'); return; }
  btn.disabled = true;
  btn.textContent = '测试中...';

  // deduplicate upstream keys
  const seen = new Set();
  const toTest = [];
  for (const k of cachedKeys) {
    if (!seen.has(k.upstream_key)) {
      seen.add(k.upstream_key);
      toTest.push(k);
    }
  }

  let okCount = 0;
  let failCount = 0;
  for (const k of toTest) {
    const testBtn = document.getElementById(`test-btn-${k.id}`);
    const resultEl = testBtn?.nextElementSibling;
    if (testBtn) { testBtn.disabled = true; testBtn.textContent = '...'; }
    if (resultEl) { resultEl.className = 'test-result loading'; resultEl.textContent = '测试中'; }
    const ok = await testUpstreamKey(k.upstream_key, testBtn, k.channel_kind || 'copilot');
    if (ok) okCount++; else failCount++;
    // mark other keys with same upstream_key
    for (const k2 of cachedKeys) {
      if (k2.id !== k.id && k2.upstream_key === k.upstream_key) {
        const r2 = document.getElementById(`test-result-${k2.id}`);
        if (r2) { r2.className = `test-result ${ok ? 'ok' : 'fail'}`; r2.textContent = ok ? '✓' : `✗`; }
      }
    }
  }

  btn.disabled = false;
  btn.textContent = '测试全部';
  toast(`测试完成: ${okCount} 通过, ${failCount} 失败`, okCount > 0 && failCount === 0 ? 'success' : failCount > 0 ? 'error' : 'info');
}

function confirmDeleteKey(id) {
  const k = cachedKeys.find(x => x.id === id);
  const name = k ? k.name : `#${id}`;
  showConfirm(`删除密钥 "${name}"？`, '此操作不可撤销。', async () => {
    try {
      await api('DELETE', `/admin/keys?id=${id}`);
      toast('密钥已删除', 'success');
      loadKeys();
    } catch(e) { toast('删除失败', 'error'); }
  });
}

// ===== KEY HISTORY =====
let currentHistoryKey = '';
let currentHistoryFilter = 'all';
let keyHistoryData = []; // merged success + error

function openKeyHistory(keyId) {
  const k = cachedKeys.find(x => x.id === keyId);
  if (!k) return;
  const keyName = k.name;
  currentHistoryKey = keyName;
  currentHistoryFilter = 'all';
  document.querySelectorAll('#panel-key-history .filter-chip').forEach(c => c.classList.toggle('active', c.dataset.filter === 'all'));
  document.getElementById('key-history-title').textContent = `${keyName} 的请求记录`;
  document.getElementById('panel-key-history').classList.add('open');
  loadKeyHistory(keyName);
}

function closeSlidePanel() {
  document.getElementById('panel-key-history').classList.remove('open');
}

async function loadKeyHistory(keyName) {
  const limit = document.getElementById('key-history-limit').value;
  const content = document.getElementById('key-history-content');
  content.innerHTML = '<div class="slide-panel-empty"><div class="spinner"></div>加载中...</div>';
  try {
    const [usage, errors] = await Promise.all([
      api('GET', appendChannel(`/admin/usage?key=${encodeURIComponent(keyName)}&limit=${limit}`)),
      api('GET', appendChannel(`/admin/errors?key=${encodeURIComponent(keyName)}&limit=${limit}`)),
    ]);
    // merge and tag
    const merged = [];
    for (const r of usage) {
      merged.push({ ...r, _type: 'success', _sortTime: new Date(r.time).getTime() });
    }
    for (const r of errors) {
      merged.push({ ...r, _type: 'error', _sortTime: new Date(r.time).getTime() });
    }
    merged.sort((a, b) => b._sortTime - a._sortTime);
    keyHistoryData = merged;
    renderKeyHistory();
  } catch(e) {
    if (e.message !== 'auth') content.innerHTML = '<div class="slide-panel-empty">加载失败</div>';
  }
}

function filterKeyHistory(filter, el) {
  currentHistoryFilter = filter;
  document.querySelectorAll('#panel-key-history .filter-chip').forEach(c => c.classList.toggle('active', c.dataset.filter === filter));
  renderKeyHistory();
}

function renderKeyHistory() {
  const content = document.getElementById('key-history-content');
  let items = keyHistoryData;
  if (currentHistoryFilter === 'success') items = items.filter(r => r._type === 'success');
  if (currentHistoryFilter === 'error') items = items.filter(r => r._type === 'error');

  document.getElementById('key-history-count').textContent = `${items.length} 条`;

  if (items.length === 0) {
    content.innerHTML = '<div class="slide-panel-empty">暂无记录</div>';
    return;
  }

  content.innerHTML = items.map((r, i) => {
    const isErr = r._type === 'error';
    const isLocal = isErr && r.is_local;
    const statusBadge = isErr
      ? `<span class="badge ${isLocal ? 'badge-purple' : r.status >= 500 ? 'badge-red' : 'badge-orange'}">${r.status}</span>`
      : '<span class="badge badge-green">OK</span>';

    let cost = '';
    if (!isErr) {
      const c = calcCostClient(r.model, r.input_tokens, r.output_tokens, r.cache_creation_tokens||0, r.cache_read_tokens||0, r.channel_kind);
      cost = '$' + c.toFixed(4);
    }

    const tokenInfo = isErr ? '' : `${fmtNum(r.input_tokens)}/${fmtNum(r.output_tokens)}`;
    const model = r.model || '';
    const ip = r.ip || '';

    // preview removed - request_body not in list response
    let preview = '';

    return `<div class="req-item">
      <div class="req-item-header" onclick="toggleHistoryDetail(${i}, ${r.id}, '${r._type}')">
        <span class="req-item-expand" id="kh-arrow-${i}">&#9654;</span>
        ${statusBadge}
        <span class="req-item-time">${fmtDateTime(r.time)}</span>
        <span class="req-item-model">${esc(model)}</span>
        <span class="req-item-tokens">${tokenInfo}</span>
        <span class="req-item-cost">${cost}</span>
      </div>
      <div class="req-item-detail" id="kh-detail-${i}">
        <div class="req-meta-grid">
          <div class="req-meta-item"><span class="meta-label">IP: </span><span class="meta-value">${esc(ip)}</span></div>
          ${!isErr ? `
            <div class="req-meta-item"><span class="meta-label">输入: </span><span class="meta-value">${r.input_tokens}</span></div>
            <div class="req-meta-item"><span class="meta-label">输出: </span><span class="meta-value">${r.output_tokens}</span></div>
            <div class="req-meta-item"><span class="meta-label">缓存写: </span><span class="meta-value">${r.cache_creation_tokens||0}</span></div>
            <div class="req-meta-item"><span class="meta-label">缓存读: </span><span class="meta-value">${r.cache_read_tokens||0}</span></div>
          ` : `
            <div class="req-meta-item"><span class="meta-label">路径: </span><span class="meta-value">${esc(r.path)}</span></div>
            <div class="req-meta-item"><span class="meta-label">状态: </span><span class="meta-value">${r.status}</span></div>
          `}
        </div>
        <div class="req-detail-section">
          <div class="req-detail-label">请求体 <button class="copy-btn" onclick="event.stopPropagation();copyReqBody(${r.id}, '${r._type}')">复制</button></div>
          <div class="req-detail-body" id="kh-body-${i}">
            <pre>点击展开加载...</pre>
          </div>
        </div>
        ${isErr ? `
        <div class="req-detail-section">
          <div class="req-detail-label">响应体</div>
          <div class="req-detail-body" id="kh-resp-${i}">
            <pre>点击展开加载...</pre>
          </div>
        </div>` : ''}
      </div>
    </div>`;
  }).join('');
}

async function toggleHistoryDetail(i, id, type) {
  const detail = document.getElementById(`kh-detail-${i}`);
  const arrow = document.getElementById(`kh-arrow-${i}`);
  if (!detail) return;
  const open = detail.classList.toggle('open');
  if (arrow) arrow.innerHTML = open ? '&#9660;' : '&#9654;';
  if (open) {
    const bodyEl = document.getElementById(`kh-body-${i}`);
    const pre = bodyEl?.querySelector('pre');
    if (pre && !bodyEl.dataset.loaded) {
      pre.textContent = '加载中...';
      try {
        const endpoint = type === 'error' ? `/admin/errors/${id}` : `/admin/usage/${id}`;
        const data = await api('GET', endpoint);
        if (data.request_body) {
          try { pre.textContent = JSON.stringify(JSON.parse(data.request_body), null, 2); }
          catch { pre.textContent = data.request_body; }
        } else {
          pre.textContent = '(空)';
        }
        if (type === 'error') {
          const respPre = document.querySelector(`#kh-resp-${i} pre`);
          if (respPre) respPre.textContent = data.response_body || '(空)';
        }
        bodyEl.dataset.loaded = '1';
      } catch { pre.textContent = '加载失败'; }
    }
  }
}

function copyText(text) {
  if (navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(text).then(() => toast('已复制', 'info')).catch(() => copyFallback(text));
  } else {
    copyFallback(text);
  }
}
function copyFallback(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;left:-9999px';
  document.body.appendChild(ta);
  ta.select();
  document.execCommand('copy');
  document.body.removeChild(ta);
  toast('已复制', 'info');
}

async function copyReqBody(id, type) {
  try {
    const endpoint = type === 'error' ? `/admin/errors/${id}` : `/admin/usage/${id}`;
    const data = await api('GET', endpoint);
    if (!data.request_body) { toast('请求体为空', 'info'); return; }
    let formatted;
    try { formatted = JSON.stringify(JSON.parse(data.request_body), null, 2); }
    catch { formatted = data.request_body; }
    copyText(formatted);
  } catch {
    toast('复制失败', 'error');
  }
}

function fmtDateTime(t) {
  if (!t) return '';
  try {
    return new Date(t).toLocaleString('zh-CN', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, timeZone: 'Asia/Shanghai' });
  } catch { return t.slice(0, 19); }
}

// ===== USAGE =====
async function loadUsage() {
  const key = document.getElementById('usage-key-filter').value;
  const limit = document.getElementById('usage-limit').value;
  let path = `/admin/usage?limit=${limit}`;
  if (key) path += `&key=${encodeURIComponent(key)}`;
  try {
    usageData = await api('GET', appendChannel(path));
    renderUsage(usageData);
  } catch(e) { if (e.message !== 'auth') toast('加载使用记录失败', 'error'); }
}

function renderUsage(rows) {
  const tbody = document.getElementById('usage-tbody');
  tbody.innerHTML = rows.map((r, i) => {
    const cost = calcCostClient(r.model, r.input_tokens, r.output_tokens, r.cache_creation_tokens, r.cache_read_tokens, r.channel_kind);
    return `<tr style="cursor:pointer" onclick="toggleUsageExpand(${r.id}, 'usage-expand-${i}')">
      <td style="width:20px;color:var(--text-muted)">+</td>
      <td class="mono" style="font-size:11px">${fmtTime(r.time)}</td>
      <td>${channelTag(r.channel_kind)}</td>
      <td>${esc(r.key_name)}</td>
      <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis">${esc(r.model)}</td>
      <td class="mono">${fmtNum(r.input_tokens)}</td>
      <td class="mono">${fmtNum(r.output_tokens)}</td>
      <td class="mono">${fmtNum(r.cache_creation_tokens)}</td>
      <td class="mono">${fmtNum(r.cache_read_tokens)}</td>
      <td class="mono">$${cost.toFixed(4)}</td>
      <td class="mono" style="font-size:11px">${esc(r.ip || '')}</td>
    </tr>
    <tr class="expand-row" id="usage-expand-${i}" style="display:none">
      <td colspan="11"><pre>加载中...</pre></td>
    </tr>`;
  }).join('');
}

async function toggleUsageExpand(id, elId) {
  await lazyToggle(elId, `/admin/usage/${id}`, 'usage');
}

async function toggleErrorExpand(id, elId) {
  await lazyToggle(elId, `/admin/errors/${id}`, 'error');
}

async function lazyToggle(elId, endpoint, type) {
  const el = document.getElementById(elId);
  if (!el) return;
  if (el.style.display === 'none') {
    el.style.display = '';
    const pre = el.querySelector('pre');
    if (pre && !el.dataset.loaded) {
      pre.textContent = '加载中...';
      try {
        const data = await api('GET', endpoint);
        let parsed;
        try { parsed = JSON.stringify(JSON.parse(data.request_body), null, 2); }
        catch { parsed = data.request_body || '(空)'; }
        pre.textContent = parsed;
        if (type === 'error') {
          const td = el.querySelector('td');
          const respBody = data.response_body || '(空)';
          td.innerHTML = `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
            <div><div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">请求体</div><pre>${esc(pre.textContent)}</pre></div>
            <div><div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">响应体</div><pre>${esc(respBody)}</pre></div>
          </div>`;
        }
        el.dataset.loaded = '1';
      } catch { pre.textContent = '加载失败'; }
    }
  } else { el.style.display = 'none'; }
}

function exportUsage() {
  const key = document.getElementById('usage-key-filter').value;
  let path = `/admin/usage/export?token=${encodeURIComponent(KEY)}`;
  if (key) path += `&key=${encodeURIComponent(key)}`;
  toast('正在导出（含请求体，文件较大请耐心等待）...', 'info');
  window.open(appendChannel(path), '_blank');
}

function exportKeyUsage(keyId) {
  const k = cachedKeys.find(x => x.id === keyId);
  if (!k) return;
  const keyName = k.name;
  toast(`正在导出 ${keyName} 的数据...`, 'info');
  window.open(appendChannel(`/admin/usage/export?token=${encodeURIComponent(KEY)}&key=${encodeURIComponent(keyName)}`), '_blank');
}

// ===== ERRORS =====
async function loadErrors() {
  const key = document.getElementById('error-key-filter').value;
  const limit = document.getElementById('error-limit').value;
  let path = `/admin/errors?limit=${limit}`;
  if (key) path += `&key=${encodeURIComponent(key)}`;
  try {
    const rows = await api('GET', appendChannel(path));
    renderErrors(rows);
  } catch(e) { if (e.message !== 'auth') toast('加载错误日志失败', 'error'); }
}

function renderErrors(rows) {
  const tbody = document.getElementById('error-tbody');
  tbody.innerHTML = rows.map((r, i) => {
    const s = r.status;
    let badge = 'badge-orange';
    if (s === 429 || s === 0) badge = 'badge-purple';
    else if (s >= 500) badge = 'badge-red';
    return `<tr style="cursor:pointer" onclick="toggleErrorExpand(${r.id}, 'err-expand-${i}')">
      <td style="width:20px;color:var(--text-muted)">+</td>
      <td class="mono" style="font-size:11px">${fmtTime(r.time)}</td>
      <td>${channelTag(r.channel_kind)}</td>
      <td>${esc(r.key_name)}</td>
      <td><span class="badge ${badge}">${s}</span></td>
      <td class="mono" style="font-size:11px">${esc(r.path)}</td>
      <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis">${esc(r.model)}</td>
      <td class="mono" style="font-size:11px">${esc(r.ip || '')}</td>
    </tr>
    <tr class="expand-row" id="err-expand-${i}" style="display:none">
      <td colspan="8"><pre>加载中...</pre></td>
    </tr>`;
  }).join('');
}

function confirmClearErrors() {
  // 当前渠道有过滤就按渠道删；'全部' 走 ?confirm=yes 显式确认，防 curl 手抖。
  const scope = currentChannel === 'all' ? '?confirm=yes' : `?channel=${encodeURIComponent(currentChannel)}`;
  showConfirm('清空所有错误？', '此操作不可撤销。', async () => {
    try {
      await api('DELETE', '/admin/errors' + scope);
      toast('错误日志已清空', 'success');
      loadErrors();
    } catch(e) { toast('操作失败', 'error'); }
  });
}

// ===== MAP =====
let geoCache;
try { geoCache = JSON.parse(localStorage.getItem('geoCache') || '{}'); } catch { geoCache = {}; }
const GEO_TTL = 7 * 24 * 3600 * 1000;

async function loadMap() {
  if (!mapInstance) {
    mapInstance = L.map('map', { zoomControl: true }).setView([30, 0], 2);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OSM &copy; CARTO',
      maxZoom: 18,
    }).addTo(mapInstance);
    setTimeout(() => mapInstance.invalidateSize(), 100);
    setTimeout(() => mapInstance.invalidateSize(), 500);
  }

  try {
    const ips = await api('GET', appendChannel('/admin/usage/ips'));
    renderMapSidebar(ips);
    await plotIPs(ips);
  } catch(e) { if (e.message !== 'auth') toast('加载 IP 数据失败', 'error'); }
}

function renderMapSidebar(ips) {
  const total = ips.reduce((s, r) => s + r.request_count, 0);
  document.getElementById('map-sidebar').innerHTML = `
    <div class="map-stat-card">
      <h4>概览</h4>
      <div style="font-family:var(--mono);font-size:20px;font-weight:600;margin-bottom:4px">${ips.length}</div>
      <div style="font-size:12px;color:var(--text-muted)">独立 IP / ${fmtNum(total)} 次请求</div>
    </div>
    <div class="map-stat-card">
      <h4>Top IP</h4>
      ${ips.slice(0, 10).map(ip => `
        <div class="ip-list-item">
          <span class="ip-addr">${esc(ip.ip)}</span>
          <span class="badge badge-blue">${ip.request_count}</span>
        </div>`).join('')}
      ${ips.length === 0 ? '<div style="font-size:12px;color:var(--text-muted)">暂无数据</div>' : ''}
    </div>`;
}

async function plotIPs(ips) {
  mapMarkers.forEach(m => m.remove());
  mapMarkers = [];

  const toGeolocate = ips.filter(ip => {
    if (ip.ip === '127.0.0.1' || ip.ip === '::1' || ip.ip.startsWith('192.168.') || ip.ip.startsWith('10.')) return false;
    const cached = geoCache[ip.ip];
    if (cached && Date.now() - cached.ts < GEO_TTL) return false;
    return true;
  });

  // batch geolocation with delay
  for (let i = 0; i < Math.min(toGeolocate.length, 40); i++) {
    const ip = toGeolocate[i].ip;
    try {
      const r = await api('GET', `/admin/geoip?ip=${encodeURIComponent(ip)}`);
      const d = r;
      if (d.status === 'success') {
        geoCache[ip] = { lat: d.lat, lon: d.lon, country: d.country, city: d.city, ts: Date.now() };
      }
      if (i % 10 === 9) await sleep(1500); // rate limit
    } catch {}
  }
  try { localStorage.setItem('geoCache', JSON.stringify(geoCache)); } catch {}

  const maxCount = Math.max(...ips.map(r => r.request_count), 1);
  for (const row of ips) {
    const geo = geoCache[row.ip];
    if (!geo || !geo.lat) continue;
    const radius = Math.max(6, Math.min(30, (row.request_count / maxCount) * 30));
    const marker = L.circleMarker([geo.lat, geo.lon], {
      radius,
      fillColor: mapMarkerColor,
      fillOpacity: 0.5,
      color: mapMarkerColor,
      weight: 1,
    }).addTo(mapInstance);
    marker.bindPopup(`
      <div class="mono">${esc(row.ip)}</div>
      <div>${geo.city || ''}, ${geo.country || ''}</div>
      <div>${row.request_count} 次请求</div>
      <div>${row.keys_used} 个 Key</div>
      <div style="font-size:10px;color:var(--text-muted)">${fmtDate(row.first_seen)} ~ ${fmtDate(row.last_seen)}</div>
    `);
    mapMarkers.push(marker);
  }
}

// ===== SYSTEM =====
function loadSystem() {
  document.getElementById('system-session-info').textContent = `Key: ${KEY.slice(0, 8)}...${KEY.slice(-4)}`;
  testAuth().then(ok => {
    document.getElementById('system-conn-info').innerHTML = ok
      ? `<span style="color:var(--green)">已连接</span> ${API}`
      : `<span style="color:var(--red)">已断开</span>`;
  });
  loadUpstreamKeys();
}

// ===== UPSTREAM KEYS =====
let cachedUpstream = [];
let cachedBreaker = []; // 熔断器状态缓存

async function loadUpstreamKeys() {
  try {
    const [rows, br] = await Promise.all([
      api('GET', appendChannel('/admin/upstream')),
      api('GET', appendChannel('/admin/upstream/breaker')),
    ]);
    cachedUpstream = rows;
    cachedBreaker = br;
    renderUpstreamKeys(rows);
  } catch(e) { if (e.message !== 'auth') toast('加载上游密钥失败', 'error'); }
}

function renderBreakerCell(id, br) {
  if (br && br.disabled) {
    return `<span class="badge badge-red">已熔断 (${br.count})</span>
      <button class="btn btn-sm" style="margin-left:4px;padding:2px 8px;font-size:10px" onclick="breakerAction(${id},'reset')">恢复</button>`;
  }
  if (br && br.count > 0) {
    return `<span class="badge badge-yellow">429×${br.count}</span>
      <button class="btn btn-sm btn-danger" style="margin-left:4px;padding:2px 8px;font-size:10px" onclick="breakerAction(${id},'disable')">关闭</button>`;
  }
  return '<span style="color:var(--text-muted);font-size:11px">正常</span>';
}

function renderUpstreamKeys(rows) {
  const tbody = document.getElementById('upstream-tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:24px">暂无上游密钥，点击"添加"创建</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => {
    const masked = r.key.length > 20 ? r.key.slice(0, 15) + '...' + r.key.slice(-4) : r.key;
    const br = cachedBreaker.find(b => b.id === r.id);
    return `<tr>
      <td class="mono">${r.id}</td>
      <td>${esc(r.name)}</td>
      <td>${channelTag(r.channel_kind)}</td>
      <td>
        <span class="key-masked" onclick="toggleKeyReveal(this,'${esc(r.key)}')" title="点击显示/复制">${esc(masked)}</span>
        <button class="btn btn-sm" style="margin-left:4px;padding:2px 6px;font-size:10px" onclick="copyText('${esc(r.key)}')">复制</button>
        <button class="btn btn-sm" style="margin-left:2px;padding:2px 6px;font-size:10px" onclick="testUpstreamKey('${esc(r.key)}',this,'${esc(r.channel_kind || 'copilot')}')" id="test-upstream-${r.id}">测试</button>
        <span class="test-result" id="test-upstream-result-${r.id}"></span>
      </td>
      <td>
        <span class="badge ${r.enabled ? 'badge-green' : 'badge-red'}" style="cursor:pointer" onclick="toggleUpstreamEnabled(${r.id},${r.enabled})">${r.enabled ? '启用' : '禁用'}</span>${!r.enabled && r.note ? `<span style="color:var(--text-muted);font-size:11px;margin-left:4px">${esc(r.note)}</span>` : ''}
      </td>
      <td id="breaker-cell-${r.id}">${renderBreakerCell(r.id, br)}</td>
      <td class="mono" style="font-size:11px">${fmtDate(r.created_at)}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm" onclick="editUpstream(${r.id})">编辑</button>
        <button class="btn btn-sm btn-danger" onclick="confirmDeleteUpstream(${r.id})">删除</button>
      </td>
    </tr>`;
  }).join('');
}

function syncUpstreamChannel() {
  applyChannelToModal(document.getElementById('au-channel').value, {
    keyInputId: 'au-key',
    keyLabelId: 'au-key-label',
    labelTemplate: '密钥（{placeholder}）',
  });
}

function showAddUpstreamModal() {
  document.getElementById('au-id').value = '';
  document.getElementById('au-name').value = '';
  document.getElementById('au-key').value = '';
  document.getElementById('au-note').value = '';
  const defaultChannel = currentChannel === 'anthropic' ? 'anthropic' : 'copilot';
  document.getElementById('au-channel').value = defaultChannel;
  syncUpstreamChannel();
  document.getElementById('upstream-modal-title').textContent = '添加上游密钥';
  openModal('modal-add-upstream');
}

function editUpstream(id) {
  const r = cachedUpstream.find(x => x.id === id);
  if (!r) return;
  document.getElementById('au-id').value = id;
  document.getElementById('au-name').value = r.name;
  document.getElementById('au-key').value = r.key;
  document.getElementById('au-note').value = r.note || '';
  document.getElementById('au-channel').value = r.channel_kind || 'copilot';
  syncUpstreamChannel();
  document.getElementById('upstream-modal-title').textContent = '编辑上游密钥';
  openModal('modal-add-upstream');
}

async function saveUpstream() {
  const id = document.getElementById('au-id').value;
  const name = document.getElementById('au-name').value.trim();
  const key = document.getElementById('au-key').value.trim();
  const note = document.getElementById('au-note').value.trim();
  const channel = document.getElementById('au-channel').value;
  if (!key) { toast('密钥不能为空', 'error'); return; }
  try {
    if (id) {
      // 让用户改 key 时能同步改渠道，后端会校验 channel 与 key 前缀一致。
      await api('PATCH', `/admin/upstream?id=${id}`, { name, key, note, channel_kind: channel });
    } else {
      await api('POST', '/admin/upstream', { name, key, note, channel_kind: channel });
    }
    toast(id ? '已更新' : '已添加', 'success');
    closeModal('modal-add-upstream');
    loadUpstreamKeys();
  } catch(e) { toast('保存失败: ' + e.message, 'error'); }
}

async function toggleUpstreamEnabled(id, current) {
  try {
    await api('PATCH', `/admin/upstream?id=${id}`, { enabled: current ? 0 : 1 });
    toast(current ? '已禁用' : '已启用', 'success');
    loadUpstreamKeys();
  } catch(e) { toast('操作失败', 'error'); }
}

function confirmDeleteUpstream(id) {
  const r = cachedUpstream.find(x => x.id === id);
  const name = r ? r.name : `#${id}`;
  showConfirm(`删除上游密钥 "${name}"？`, '此操作不可撤销。', async () => {
    try {
      await api('DELETE', `/admin/upstream?id=${id}`);
      toast('已删除', 'success');
      loadUpstreamKeys();
    } catch(e) { toast('删除失败', 'error'); }
  });
}

async function testAllUpstream() {
  const btn = document.getElementById('btn-test-all-upstream');
  if (!cachedUpstream.length) { toast('没有上游密钥', 'error'); return; }
  btn.disabled = true;
  btn.textContent = '测试中...';
  let ok = 0, fail = 0;
  for (const u of cachedUpstream) {
    const testBtn = document.getElementById(`test-upstream-${u.id}`);
    const result = await testUpstreamKey(u.key, testBtn, u.channel_kind || 'copilot');
    if (result) ok++; else fail++;
  }
  btn.disabled = false;
  btn.textContent = '测试全部';
  toast(`测试完成: ${ok} 通过, ${fail} 失败`, fail === 0 ? 'success' : 'error');
}

async function breakerAction(id, action) {
  try {
    await api('POST', `/admin/upstream/breaker?id=${id}&action=${action}`);
    toast(action === 'reset' ? '已恢复' : '已关闭', 'success');
    // 刷新熔断状态
    const br = await api('GET', appendChannel('/admin/upstream/breaker'));
    cachedBreaker = br;
    renderUpstreamKeys(cachedUpstream);
  } catch(e) { toast('操作失败', 'error'); }
}

function updateBreakerUI(breakerData) {
  if (JSON.stringify(breakerData) === JSON.stringify(cachedBreaker)) return;
  cachedBreaker = breakerData;
  for (const u of cachedUpstream) {
    const cell = document.getElementById(`breaker-cell-${u.id}`);
    if (!cell) continue;
    const br = breakerData.find(b => b.id === u.id);
    cell.innerHTML = renderBreakerCell(u.id, br);
  }
}

function confirmResetStats() {
  showConfirm('重置所有统计？', '所有使用记录将被永久删除。', async () => {
    try {
      await api('POST', '/stats/reset');
      toast('统计已重置', 'success');
      loadOverview();
    } catch(e) { toast('操作失败', 'error'); }
  });
}

// ===== KEY FILTERS =====
function populateKeyFilters(keys) {
  const names = keys.map(k => k.name);
  for (const sel of [document.getElementById('usage-key-filter'), document.getElementById('error-key-filter')]) {
    const current = sel.value;
    sel.innerHTML = '<option value="">全部 Key</option>' + names.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
    sel.value = current;
  }
}

// ===== AUTO REFRESH =====
function toggleAutoRefresh() {
  refreshEnabled = !refreshEnabled;
  document.getElementById('refresh-toggle').classList.toggle('on', refreshEnabled);
  if (refreshEnabled) startRefresh();
  else stopRefresh();
}

function updateRefreshInterval() {
  refreshMs = parseInt(document.getElementById('refresh-interval').value);
  if (refreshEnabled) { stopRefresh(); startRefresh(); }
}

function startRefresh() {
  stopRefresh();
  refreshTimer = setInterval(async () => {
    if (document.hidden || refreshRunning) return;
    refreshRunning = true;
    try { await loadTab(activeTab); } catch {}
    refreshRunning = false;
  }, refreshMs);
}

function stopRefresh() {
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; }
}

// ===== CONN STATUS =====
function updateConnStatus(online) {
  const dot = document.getElementById('conn-dot');
  const text = document.getElementById('conn-text');
  dot.classList.toggle('offline', !online);
  text.textContent = online ? '已连接' : '已断开';
  const dotMobile = document.getElementById('conn-dot-mobile');
  if (dotMobile) dotMobile.classList.toggle('offline', !online);
}

// ===== WEBSOCKET =====
let ws = null;
let wsReconnectTimer = null;
let wsAuthFailed = false;

function connectWebSocket() {
  if (ws || wsAuthFailed) return;
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${location.host}/ws`;
  try {
    ws = new WebSocket(url);
  } catch { return; }

  ws.onopen = () => {
    ws.send(JSON.stringify({ type: 'auth', key: KEY }));
  };

  ws.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === 'auth') {
        if (data.ok) { updateConnStatus(true); if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; } }
        else { wsAuthFailed = true; ws.close(); }
        return;
      }
      applyWsSnapshot(data);
    } catch {}
  };

  ws.onclose = () => {
    ws = null;
    updateConnStatus(false);
    if (!wsAuthFailed) {
      wsReconnectTimer = setTimeout(connectWebSocket, 5000);
    }
  };

  ws.onerror = () => { ws?.close(); };
}

function applyWsSnapshot(data) {
  if (!data) return;
  const visible = pickChannelKeys(data);
  for (const k of visible) {
    const row = document.querySelector(`#keys-tbody tr td[data-col="id"]`)
      ? Array.from(document.querySelectorAll('#keys-tbody tr')).find(tr => {
          const idCell = tr.querySelector('td[data-col="id"]');
          return idCell && parseInt(idCell.textContent) === k.id;
        })
      : null;
    if (!row) continue;
    const usedCell = row.querySelector('td[data-col="used"]');
    const concCell = row.querySelector('td[data-col="current_concurrency"]');
    if (usedCell) usedCell.textContent = '$' + Number(k.used || 0).toFixed(2);
    if (concCell) concCell.textContent = String(k.current_concurrency);
  }
  if (Array.isArray(data.breaker)) {
    const filtered = currentChannel === 'all'
      ? data.breaker
      : data.breaker.filter(b => (b.channel_kind || 'copilot') === currentChannel);
    updateBreakerUI(filtered);
  }
}

// ===== HELPERS =====
function openModal(id) { document.getElementById(id).classList.add('show'); }
function closeModal(id) { document.getElementById(id).classList.remove('show'); }

function showConfirm(title, msg, onConfirm) {
  document.getElementById('confirm-title').textContent = title;
  document.getElementById('confirm-msg').textContent = msg;
  const btn = document.getElementById('confirm-btn');
  const newBtn = btn.cloneNode(true);
  btn.parentNode.replaceChild(newBtn, btn);
  newBtn.addEventListener('click', () => { closeModal('modal-confirm'); onConfirm(); });
  openModal('modal-confirm');
}

function toast(msg, type = 'info') {
  const container = document.getElementById('toasts');
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  container.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 200); }, 3000);
}

function fmtNum(n) {
  if (n == null) return '0';
  if (n >= 1_000_000) return (n/1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n/1_000).toFixed(1) + 'K';
  return String(n);
}

function fmtTime(t) {
  if (!t) return '';
  try { return new Date(t).toLocaleTimeString('zh-CN', { hour12: false, hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Shanghai' }); }
  catch { return t.slice(11, 16); }
}

function fmtDate(t) {
  if (!t) return '';
  try { return new Date(t).toLocaleDateString('zh-CN', { month: 'short', day: 'numeric', timeZone: 'Asia/Shanghai' }); }
  catch { return t.slice(0, 10); }
}

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

function isFast(m) { return (m || '').toLowerCase().includes('fast'); }

// 启动时从 /admin/pricing 拉一次，避免和后端 billing::pricing 漂移。
// 拉失败 (未登录 / 网络) 时 fallback 用一份兜底常量，但发警告。
let PRICING_CACHE = null;
const PRICING_FALLBACK = {
  copilot: {
    opus:      { input: 5,  output: 25,  cache_write: 6.25,  cache_read: 0.5 },
    opus_fast: { input: 30, output: 150, cache_write: 37.5,  cache_read: 3 },
    sonnet:    { input: 3,  output: 15,  cache_write: 3.75,  cache_read: 0.3 },
    haiku:     { input: 1,  output: 5,   cache_write: 1.25,  cache_read: 0.1 },
  },
  anthropic: {
    opus:      { input: 5,  output: 25,  cache_write: 6.25,  cache_read: 0.5 },
    sonnet:    { input: 3,  output: 15,  cache_write: 3.75,  cache_read: 0.3 },
    haiku:     { input: 1,  output: 5,   cache_write: 1.25,  cache_read: 0.1 },
  },
};

async function loadPricing() {
  try {
    PRICING_CACHE = await api('GET', '/admin/pricing');
  } catch (e) {
    console.warn('failed to load /admin/pricing, using fallback', e);
    PRICING_CACHE = PRICING_FALLBACK;
  }
}

function getClientRate(model, channelKind) {
  const table = (PRICING_CACHE || PRICING_FALLBACK)[channelKind || 'copilot'] || PRICING_FALLBACK.copilot;
  const m = (model || '').toLowerCase();
  if (m.includes('haiku'))  return table.haiku;
  if (m.includes('sonnet')) return table.sonnet;
  if (isFast(model) && table.opus_fast) return table.opus_fast;
  return table.opus;
}

function calcCostClient(model, inp, outp, cw, cr, channelKind) {
  const r = getClientRate(model, channelKind);
  return (inp/1e6)*r.input + (outp/1e6)*r.output + (cw/1e6)*r.cache_write + (cr/1e6)*r.cache_read;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ===== KEYBOARD =====
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;
  const tabs = ['overview','keys','usage','errors','map','system'];
  if (e.key >= '1' && e.key <= '6') { e.preventDefault(); switchTab(tabs[parseInt(e.key)-1]); }
  if (e.key === 'r' || e.key === 'R') { e.preventDefault(); loadTab(activeTab); }
  if (e.key === 'Escape') {
    document.querySelectorAll('.modal-overlay.show').forEach(m => m.classList.remove('show'));
    closeSlidePanel();
    const sidebar = document.getElementById('sidebar');
    if (sidebar.classList.contains('open')) toggleMobileSidebar();
  }
});

// click outside modal to close
document.querySelectorAll('.modal-overlay').forEach(overlay => {
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.classList.remove('show'); });
});

// login enter key
document.getElementById('key-input').addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });

// ===== INIT =====
(function init() {
  const saved = sessionStorage.getItem('admin_key');
  if (saved) {
    KEY = saved;
    testAuth().then(ok => {
      if (ok) showApp();
      else document.getElementById('login-screen').style.display = 'flex';
    });
  }
})();
