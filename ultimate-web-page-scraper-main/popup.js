// ═══════════════════════════════════════════════════════════════════
//  HTML Grabber Pro — popup.js
// ═══════════════════════════════════════════════════════════════════

// ── DOM refs ──────────────────────────────────────────────────────
const grabBtn       = document.getElementById('grabBtn');
const xpathBtn      = document.getElementById('xpathBtn');
const saveBtn       = document.getElementById('saveBtn');
const resetBtn      = document.getElementById('resetBtn');
const status        = document.getElementById('status');
const pageUrlEl     = document.getElementById('pageUrl');
const lastSizeEl    = document.getElementById('lastSize');
const selectorRow   = document.getElementById('selectorRow');
const selectorInput = document.getElementById('selectorInput');
const detectBanner  = document.getElementById('detectBanner');
const detectDesc    = document.getElementById('detectDesc');
const detectSections= document.getElementById('detectSections');
const historyEmpty  = document.getElementById('historyEmpty');
const historyList   = document.getElementById('historyList');
const historyClear  = document.getElementById('historyClear');
const diffA         = document.getElementById('diffA');
const diffB         = document.getElementById('diffB');
const diffBtn       = document.getElementById('diffBtn');
const diffResult    = document.getElementById('diffResult');
const pills         = document.querySelectorAll('.pill');

// ── State ─────────────────────────────────────────────────────────
let currentMode   = 'full';
let pickingActive = false;
let currentTab    = null;
let lastGrabbedHTML = null;
let lastGrabbedXPath = null;
let settings = { autoDetect: true, saveHistory: true, autoXpath: false, timestampFiles: true };

// ═══════════════════════════════════════════════════════════════════
//  INIT
// ═══════════════════════════════════════════════════════════════════
async function init() {
  await loadSettings();

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  currentTab = tab;
  if (tab?.url) pageUrlEl.textContent = tab.url;

  // Clean up any bloated history from old versions that stored full HTML
  // This frees up storage quota that was causing kQuotaBytes errors
  try {
    const data = await chrome.storage.local.get('grab_history');
    const history = data.grab_history || [];
    const needsClean = history.some(item => item.html && item.html.length > 500);
    if (needsClean) {
      const cleaned = history.map(item => ({
        mode:    item.mode,
        url:     item.url,
        sizeKB:  item.sizeKB,
        xpath:   item.xpath || null,
        ts:      item.ts,
        preview: item.html ? item.html.slice(0, 300) : (item.preview || ''),
      }));
      await chrome.storage.local.set({ grab_history: cleaned });
    }
    // Also clear any stale pending copy that might be taking up space
    await chrome.storage.local.remove('__grabber_pending_copy__');
  } catch(_) {}

  renderHistory();
  renderDiffSelects();

  if (settings.autoDetect) runAutoDetect();

  // Check if background shortcut grabbed something
  const stored = await chrome.storage.local.get('__shortcut_html__');
  if (stored.__shortcut_html__) {
    lastGrabbedHTML = stored.__shortcut_html__;
    await chrome.storage.local.remove('__shortcut_html__');
    setStatus('Shortcut grab ready — click Save or it\'s already in clipboard', 'ok');
  }
}

// ═══════════════════════════════════════════════════════════════════
//  TAB SWITCHING
// ═══════════════════════════════════════════════════════════════════
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'history') renderHistory();
    if (tab.dataset.tab === 'diff')    renderDiffSelects();
    if (tab.dataset.tab === 'stats')   initStatsTab();
  });
});

// ═══════════════════════════════════════════════════════════════════
//  MODE PILLS
// ═══════════════════════════════════════════════════════════════════
pills.forEach(pill => {
  pill.addEventListener('click', () => {
    pills.forEach(p => p.classList.remove('active'));
    pill.classList.add('active');
    currentMode = pill.dataset.mode;

    selectorRow.style.display = currentMode === 'selector' ? 'block' : 'none';

    if (currentMode === 'full') {
      grabBtn.textContent = '⚡ Copy HTML'; grabBtn.className = 'grab-btn';
      setStatus('');
    } else if (currentMode === 'pick') {
      grabBtn.textContent = '🎯 Start Picking'; grabBtn.className = 'grab-btn';
      setStatus('Click Start Picking, then click any element on the page', 'warn');
    } else if (currentMode === 'selector') {
      grabBtn.textContent = '⚡ Copy HTML'; grabBtn.className = 'grab-btn';
      setStatus('Enter a CSS selector above', 'warn');
    }

    if (currentMode !== 'pick' && pickingActive) cancelPick();
  });
});

// ═══════════════════════════════════════════════════════════════════
//  MAIN GRAB BUTTON
// ═══════════════════════════════════════════════════════════════════
grabBtn.addEventListener('click', async () => {
  if (currentMode === 'full')     return grabFull();
  if (currentMode === 'selector') return grabBySelector();
  if (currentMode === 'pick')     return startPick();
});

// ── XPATH BUTTON ─────────────────────────────────────────────────
xpathBtn.addEventListener('click', async () => {
  if (!lastGrabbedHTML && currentMode !== 'pick') {
    setStatus('Grab something first, then copy its XPath', 'warn'); return;
  }
  if (lastGrabbedXPath) {
    await navigator.clipboard.writeText(lastGrabbedXPath);
    flashBtn(xpathBtn, '✓ XPath copied!');
    setStatus('XPath copied to clipboard', 'ok');
  } else {
    setStatus('XPath only available after picking an element', 'warn');
  }
});

// ── SAVE FILE BUTTON ──────────────────────────────────────────────
saveBtn.addEventListener('click', async () => {
  if (!lastGrabbedHTML) { setStatus('Nothing grabbed yet', 'warn'); return; }
  downloadHTML(lastGrabbedHTML, currentTab?.url || 'page');
  flashBtn(saveBtn, '✓ Saved!');
});

// ── RESET BUTTON ──────────────────────────────────────────────────
resetBtn.addEventListener('click', async () => {
  // 1. Kill any active picker on the page
  if (pickingActive) {
    pickingActive = false;
    try {
      await chrome.scripting.executeScript({
        target: { tabId: currentTab.id },
        func: () => {
          sessionStorage.removeItem('__htmlgrabber_picked__');
          sessionStorage.removeItem('__htmlgrabber_cancelled__');
          const overlay = document.getElementById('__htmlgrabber_overlay__');
          if (overlay) overlay.remove();
          // remove highlight div too
          document.querySelectorAll('[style*="2147483646"]').forEach(el => el.remove());
        },
      });
    } catch(_) {}
  }

  // 2. Reset all UI state
  currentMode     = 'full';
  lastGrabbedHTML  = null;
  lastGrabbedXPath = null;

  pills.forEach(p => p.classList.remove('active'));
  document.querySelector('[data-mode="full"]').classList.add('active');
  selectorRow.style.display   = 'none';
  selectorInput.value         = '';
  detectBanner.style.display  = 'none';

  grabBtn.className   = 'grab-btn';
  grabBtn.textContent = '⚡ Copy HTML';

  setStatus('');
  lastSizeEl.textContent = '—';

  // 3. Flash feedback
  resetBtn.style.color       = '#00ff88';
  resetBtn.style.borderColor = '#00ff88';
  resetBtn.textContent       = '✓ Reset';
  setTimeout(() => {
    resetBtn.style.color       = '#ff4444';
    resetBtn.style.borderColor = '#330000';
    resetBtn.textContent       = '↺ Reset';
  }, 1500);
});

// ═══════════════════════════════════════════════════════════════════
//  CHAT ANNOTATOR — runs BEFORE serialization
//  Reads the DOM class names and data to inject clear semantic
//  data-* attributes onto every chat message bubble:
//    data-sender="fake|client"
//    data-gender="female|male"
//    data-persona="Silke" (fake account name)
//    data-is-last="true" (on the last/most recent message)
//    data-moderator="TT_WA024" (moderator ID if visible)
//    data-timestamp="06:57 (5 hours ago)"
//  This way the extractor never has to guess from CSS.
// ═══════════════════════════════════════════════════════════════════
function annotateChatMessages() {
  // ── Detect persona name from fake sidebar ──
  let personaName = '';
  let personaGender = '';

  // Right sidebar has id="fake-sidebar" or bg-gradient from-female/from-male
  const fakeSidebar = document.getElementById('fake-sidebar')
    || document.querySelector('[class*="fake-sidebar"]')
    || (() => {
      // fallback: find aside with female gradient (pink)
      return Array.from(document.querySelectorAll('aside')).find(a =>
        a.className && (a.className.includes('from-female') || a.className.includes('right-0'))
      );
    })();

  if (fakeSidebar) {
    // Gender from gradient class
    if (fakeSidebar.innerHTML.includes('from-female')) personaGender = 'female';
    else if (fakeSidebar.innerHTML.includes('from-male')) personaGender = 'male';

    // Name from the <p class="text-lg"> inside the sidebar header
    const nameEl = fakeSidebar.querySelector('p.text-lg, [class*="text-lg"]');
    if (nameEl) personaName = nameEl.textContent.replace(/[,\s]+$/, '').trim();
  }

  // ── Detect client name from left sidebar ──
  let clientName = '';
  let clientGender = '';
  const clientSidebar = document.getElementById('regular-sidebar')
    || (() => {
      return Array.from(document.querySelectorAll('aside')).find(a =>
        a.className && (a.className.includes('regular-sidebar') || a.className.includes('left-0') || a.className.includes('translate-x-[-336px]'))
      );
    })();

  if (clientSidebar) {
    if (clientSidebar.innerHTML.includes('from-female')) clientGender = 'female';
    else if (clientSidebar.innerHTML.includes('from-male')) clientGender = 'male';
    const nameEl = clientSidebar.querySelector('p.text-lg, [class*="text-lg"]');
    if (nameEl) clientName = nameEl.textContent.replace(/[,\s]+$/, '').trim();
  }

  // ── Find all chat message bubbles ──
  // Bubbles have class "from-female" or "from-male" AND either "ml-auto" (fake/right) or "mr-auto" (client/left)
  const allBubbles = Array.from(document.querySelectorAll('[class*="from-female"],[class*="from-male"]'))
    .filter(el => {
      const cls = el.className || '';
      return (cls.includes('from-female') || cls.includes('from-male'))
          && (cls.includes('ml-auto') || cls.includes('mr-auto') || cls.includes('rounded-tl') || cls.includes('rounded-tr'));
    });

  // ── Annotate each bubble ──
  allBubbles.forEach((bubble, index) => {
    const cls = bubble.className || '';

    // Sender: ml-auto = pushed right = fake; mr-auto = pushed left = client
    // Also: rounded-tl (top-left rounded) = fake (right side); rounded-tr = client (left side)
    const isFake = cls.includes('ml-auto') || cls.includes('rounded-tl') || cls.includes('from-female ml') ;
    const isClient = cls.includes('mr-auto') || cls.includes('rounded-tr');

    // Gender from gradient
    const gender = cls.includes('from-female') ? 'female'
                 : cls.includes('from-male')   ? 'male'
                 : '';

    bubble.setAttribute('data-sender', isFake ? 'fake' : isClient ? 'client' : 'unknown');
    bubble.setAttribute('data-gender', gender);

    if (isFake && personaName) bubble.setAttribute('data-persona', personaName);
    if (isClient && clientName) bubble.setAttribute('data-client', clientName);

    // Timestamp — look for <small> inside the bubble
    const small = bubble.querySelector('small');
    if (small) bubble.setAttribute('data-timestamp', small.textContent.trim());

    // Moderator name — the <div class="ml-auto text-right"> AFTER the bubble (persona label)
    const next = bubble.nextElementSibling;
    if (next && next.className && next.className.includes('ml-auto') && next.className.includes('text-right')) {
      bubble.setAttribute('data-moderator', next.textContent.trim());
    }
  });

  // ── Mark last message (first in DOM since column-reverse) ──
  if (allBubbles.length > 0) {
    // Column-reverse means first in DOM = newest = last sent
    allBubbles[0].setAttribute('data-is-last', 'true');
  }

  return {
    personaName,
    personaGender,
    clientName,
    clientGender,
    totalMessages: allBubbles.length,
  };
}

// ═══════════════════════════════════════════════════════════════════
//  HTML SERIALIZER WITH INLINE COMPUTED STYLES
//  Matches the "right" output format exactly:
//  - Only meaningful, non-default CSS values
//  - SVG elements stripped completely
//  - data-sender/data-gender/data-persona attributes injected
//  - No &quot; escaping inside style values — use single quotes
//  - Clean indented HTML with proper closing tags
// ═══════════════════════════════════════════════════════════════════

// Injected into the page via executeScript.
// args[0] = CSS selector string, or null for full body.
function serializeAccessibilityTree(rootSelector) {

  // Tags we never output — includes ALL SVG elements
  const SKIP_TAGS = new Set([
    'script','style','noscript','meta','link','head','template','slot',
    'svg','path','circle','rect','line','polyline','polygon','ellipse',
    'g','defs','use','symbol','clippath','lineargradient','radialgradient',
    'stop','pattern','marker','text','tspan','textpath','filter',
    'fegaussianblur','fecolormatrix','feblend','fecomposite',
  ]);

  // Void elements — self-closing, no children
  const VOID_TAGS = new Set([
    'area','base','br','col','embed','hr','img','input',
    'param','source','track','wbr',
  ]);

  // ONLY these CSS properties make it into the output.
  // This exact list matches the "right" rip-page extension output.
  const STYLE_PROPS = [
    'display',
    'overflow','overflow-x','overflow-y',
    'flex-direction','flex-wrap','flex','flex-grow','flex-shrink',
    'justify-content','align-items','align-self','gap',
    'position','top','right','bottom','left','z-index',
    'width','height','min-width','max-width','min-height','max-height',
    'padding','padding-top','padding-right','padding-bottom','padding-left',
    'margin','margin-top','margin-right','margin-bottom','margin-left',
    'border','border-top','border-right','border-bottom','border-left',
    'border-radius','border-collapse','border-color','border-width',
    'background-color','background-image','background-size','background-position',
    'backdrop-filter',
    'color','font-family','font-size','font-weight','font-style',
    'line-height','letter-spacing','text-align','text-decoration','text-transform',
    'white-space','word-break','overflow-wrap','vertical-align',
    'cursor','opacity','resize',
    'box-sizing','box-shadow',
    'user-select','list-style',
    'transform',
    'transition-property','transition-timing-function','transition-duration',
    'aspect-ratio',
  ];

  // Default / noise values to skip — BUT NOT 'auto' for margins
  // because margin-left:auto / margin-right:auto tells us client vs fake
  const ALWAYS_SKIP = new Set([
    '','initial','unset','inherit','revert',
    'rgba(0, 0, 0, 0)',
    'ease','all','0s','normal','none',
    'repeat','scroll','padding-box',
    'outside none disc','outside none none',
  ]);

  // Per-property skip rules — values that are the boring default for that prop
  const PROP_DEFAULTS = {
    'display': 'inline',
    'position': 'static',
    'overflow': 'visible', 'overflow-x': 'visible', 'overflow-y': 'visible',
    'flex-direction': 'row', 'flex-wrap': 'nowrap',
    'flex-grow': '0', 'flex-shrink': '1',
    'top': '0px', 'right': '0px', 'bottom': '0px', 'left': '0px',
    'z-index': 'auto',
    'opacity': '1',
    'border-collapse': 'separate',
    'vertical-align': 'baseline',
    'text-align': 'start',
    'text-decoration': 'none solid rgb(0, 0, 0)',
    'text-transform': 'none',
    'white-space': 'normal',
    'word-break': 'normal',
    'overflow-wrap': 'normal',
    'cursor': 'auto',
    'resize': 'none',
    'box-shadow': 'none',
    'backdrop-filter': 'none',
    'transform': 'none',
    'letter-spacing': 'normal',
    'aspect-ratio': 'auto',
    'list-style': 'outside none disc',
    'background-image': 'none',
    'background-size': 'auto',
    'background-position': '0% 0%',
    'user-select': 'auto',
    'object-fit': 'fill',
    'object-position': '50% 50%',
  };

  function buildStyleAttr(el) {
    const computed = window.getComputedStyle(el);
    const original = el.getAttribute('style') || '';

    // Start with original inline styles — they have the author's intent
    // including CSS variables, shorthands, and exact values
    const map = new Map();
    if (original) {
      original.split(';').forEach(decl => {
        const colon = decl.indexOf(':');
        if (colon === -1) return;
        const key = decl.slice(0, colon).trim();
        const val = decl.slice(colon + 1).trim();
        if (key && val) map.set(key, val);
      });
    }

    // Add computed values for props NOT already in original
    STYLE_PROPS.forEach(prop => {
      if (map.has(prop)) return; // original already has it
      const val = computed.getPropertyValue(prop).trim();
      if (!val) return;
      if (ALWAYS_SKIP.has(val)) return;
      if (PROP_DEFAULTS[prop] === val) return;
      // Skip 0px for padding/margin/border unless it's meaningful
      if (val === '0px' && (prop.startsWith('padding') || prop.startsWith('margin') || prop === 'border-width')) return;
      // Skip transparent background-color
      if (prop === 'background-color' && val === 'rgba(0, 0, 0, 0)') return;
      if (prop === 'background-color' && val === 'transparent') return;
      map.set(prop, val);
    });

    if (!map.size) return '';

    // Build style string — use single quotes inside values to avoid &quot; pollution
    return Array.from(map.entries())
      .map(([k, v]) => `${k}:${v}`)
      .join(';');
  }

  function escText(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  // For style attribute values — only escape & and use single quotes for inner strings
  // This avoids the &quot; ugliness while keeping valid HTML
  function escStyleVal(str) {
    // Replace double-quote wrapping of font-family names with single quotes
    return str.replace(/"/g, "'");
  }

  function serialize(node, depth) {
    const indent = '    '.repeat(depth);

    if (node.nodeType === 3) {
      const t = node.textContent.replace(/\s+/g, ' ').trim();
      return t ? indent + escText(t) + '\n' : '';
    }

    if (node.nodeType !== 1) return '';

    const tag = node.tagName.toLowerCase();
    if (SKIP_TAGS.has(tag)) return '';

    // Build attributes
    const attrParts = [];

    const styleStr = buildStyleAttr(node);
    if (styleStr) {
      attrParts.push(`style="${escStyleVal(styleStr)}"`);
    }

    const HTML_ATTRS = [
      'src','srcset','alt','href',
      'placeholder','rows','cols','type','name',
      'width','height','loading','decoding','data-nimg',
      // semantic chat annotations injected by annotateChatMessages()
      'data-sender','data-gender','data-persona','data-client',
      'data-is-last','data-moderator','data-timestamp',
      'id','role','aria-label',
    ];
    HTML_ATTRS.forEach(a => {
      const v = node.getAttribute(a);
      if (v !== null && v.trim() !== '') {
        attrParts.push(`${a}="${v.trim().replace(/"/g, '&quot;')}"`);
      }
    });

    // Live form values — many apps (this ExtJS panel included) set
    // .value via JS after load without touching the DOM attribute or
    // any child text node, so getAttribute('value') / textContent
    // silently comes back empty even though the field is visibly filled.
    if (tag === 'input') {
      const type = (node.getAttribute('type') || 'text').toLowerCase();
      if (type === 'checkbox' || type === 'radio') {
        if (node.checked) attrParts.push('checked="checked"');
      }
      if (node.value !== '') {
        attrParts.push(`value="${node.value.replace(/"/g, '&quot;')}"`);
      }
    } else if (tag === 'select') {
      const opt = node.options[node.selectedIndex];
      if (opt) {
        const sel = (opt.value || opt.textContent).trim();
        if (sel) attrParts.push(`data-selected-value="${sel.replace(/"/g, '&quot;')}"`);
      }
    } else if (tag === 'option') {
      if (node.selected) attrParts.push('selected="selected"');
      if (node.value !== '') attrParts.push(`value="${node.value.replace(/"/g, '&quot;')}"`);
    }

    const attrsStr = attrParts.length ? ' ' + attrParts.join(' ') : '';

    if (VOID_TAGS.has(tag)) {
      return `${indent}<${tag}${attrsStr} />\n`;
    }

    // <textarea> content lives in the .value property — it is NOT
    // reliably reflected as child text nodes once JS has set it.
    if (tag === 'textarea') {
      const val = node.value || '';
      return `${indent}<${tag}${attrsStr}>${escText(val)}</${tag}>\n`;
    }

    const childNodes = Array.from(node.childNodes);

    // Filter out skip-tag children for textOnly check
    const visibleChildren = childNodes.filter(c =>
      !(c.nodeType === 1 && SKIP_TAGS.has(c.tagName.toLowerCase()))
    );
    const textOnly = visibleChildren.every(c => c.nodeType === 3);
    const textContent = node.textContent.replace(/\s+/g, ' ').trim();

    if (textOnly && textContent) {
      return `${indent}<${tag}${attrsStr}>${escText(textContent)}</${tag}>\n`;
    }

    let out = `${indent}<${tag}${attrsStr}>\n`;
    for (const child of childNodes) {
      out += serialize(child, depth + 1);
    }
    out += `${indent}</${tag}>\n`;
    return out;
  }

  const root = rootSelector
    ? document.querySelector(rootSelector)
    : document.body;

  if (!root) return null;

  // Run chat annotator — injects data-sender, data-gender, data-persona etc.
  // onto every message bubble BEFORE we serialize, so they appear in the output
  try { annotateChatMessages(); } catch(_) {}

  if (!rootSelector) {
    return `<html>\n\n<head></head>\n\n` + serialize(root, 0) + `\n</html>`;
  }
  return serialize(root, 0);
}

// ═══════════════════════════════════════════════════════════════════
//  GRAB MODES
// ═══════════════════════════════════════════════════════════════════

async function grabFull() {
  setLoading('⏳ Serializing HTML + styles...');
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId: currentTab.id },
      func: serializeAccessibilityTree,
      args: [null],
    });
    const tree = results[0]?.result;
    if (!tree) throw new Error('No content returned.');
    await finishGrab(tree, 'Full page + inline styles', null);
  } catch (e) { showError(e.message); }
}

async function grabBySelector() {
  const sel = selectorInput.value.trim();
  if (!sel) { setStatus('Enter a CSS selector first', 'err'); return; }
  setLoading('⏳ Serializing HTML + styles...');
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId: currentTab.id },
      func: serializeAccessibilityTree,
      args: [sel],
    });
    const tree = results[0]?.result;
    if (!tree) throw new Error(`No element found for: "${sel}"`);

    // also grab xpath
    const xpathResult = await chrome.scripting.executeScript({
      target: { tabId: currentTab.id },
      func: (selector) => {
        const el = document.querySelector(selector);
        if (!el) return null;
        if (el.id) return `//*[@id="${el.id}"]`;
        const parts = []; let node = el;
        while (node && node.nodeType === 1) {
          let idx = 1, sib = node.previousSibling;
          while (sib) { if (sib.nodeType === 1 && sib.tagName === node.tagName) idx++; sib = sib.previousSibling; }
          parts.unshift(`${node.tagName.toLowerCase()}[${idx}]`);
          node = node.parentNode;
        }
        return '/' + parts.join('/');
      },
      args: [sel],
    });
    await finishGrab(tree, `Selector: ${sel}`, xpathResult[0]?.result || null);
  } catch (e) { showError(e.message); }
}

async function startPick() {
  pickingActive = true;
  grabBtn.className = 'grab-btn waiting';
  grabBtn.textContent = '⏳ Waiting for pick...';
  setStatus('Switch to the page and click an element  |  ESC to cancel', 'warn');

  await chrome.scripting.executeScript({
    target: { tabId: currentTab.id },
    func: injectPicker,
  });

  pollForPickResult(currentTab.id);
}

function pollForPickResult(tabId) {
  const interval = setInterval(async () => {
    if (!pickingActive) { clearInterval(interval); return; }
    try {
      const picked = await chrome.scripting.executeScript({
        target: { tabId },
        func: () => {
          const v = sessionStorage.getItem('__htmlgrabber_picked__');
          if (v) sessionStorage.removeItem('__htmlgrabber_picked__');
          return v ? JSON.parse(v) : null;
        },
      });
      const result = picked[0]?.result;
      if (result) {
        clearInterval(interval);
        pickingActive = false;
        await finishGrab(result.html, 'Picked element', result.xpath);
        if (settings.autoXpath && result.xpath) {
          await navigator.clipboard.writeText(result.xpath);
          setStatus(`HTML + XPath copied! XPath: ${result.xpath.slice(0, 40)}...`, 'ok');
        }
        return;
      }
      const cancelled = await chrome.scripting.executeScript({
        target: { tabId },
        func: () => { const v = sessionStorage.getItem('__htmlgrabber_cancelled__'); if(v) sessionStorage.removeItem('__htmlgrabber_cancelled__'); return v; },
      });
      if (cancelled[0]?.result) { clearInterval(interval); cancelPick(); }
    } catch(_) { clearInterval(interval); cancelPick(); }
  }, 350);
}

function cancelPick() {
  pickingActive = false;
  grabBtn.className = 'grab-btn';
  grabBtn.textContent = '🎯 Start Picking';
  setStatus('Pick cancelled', 'warn');
}

// ═══════════════════════════════════════════════════════════════════
//  FINISH GRAB — copy using textarea execCommand (works without focus)
//  Auto-clears storage after every copy so quota never fills up
// ═══════════════════════════════════════════════════════════════════
async function finishGrab(html, mode, xpath) {
  lastGrabbedHTML  = html;
  lastGrabbedXPath = xpath || null;

  const sizeKB = (new Blob([html]).size / 1024).toFixed(1);
  lastSizeEl.textContent = `${sizeKB} KB`;

  if (settings.saveHistory) {
    await addToHistory({ html, mode, xpath, url: currentTab?.url || '', sizeKB, ts: Date.now() });
  }

  // Always clear stale storage first so we start with a clean slate
  await chrome.storage.local.remove('__grabber_pending_copy__');

  let copied = false;
  try {
    // Store HTML for the injected script to read
    await chrome.storage.local.set({ __grabber_pending_copy__: html });

    // Inject into page — reads from storage, copies via textarea trick
    const results = await chrome.scripting.executeScript({
      target: { tabId: currentTab.id },
      func: () => {
        return new Promise((resolve) => {
          chrome.storage.local.get('__grabber_pending_copy__', (data) => {
            const text = data.__grabber_pending_copy__;

            // Always clean up storage immediately after reading — no leftovers
            chrome.storage.local.remove('__grabber_pending_copy__');

            if (!text) { resolve(false); return; }

            // Method 1: modern clipboard API
            if (navigator.clipboard && navigator.clipboard.writeText) {
              navigator.clipboard.writeText(text)
                .then(() => resolve(true))
                .catch(() => {
                  // Method 2: textarea + execCommand (works without focus)
                  const ta = document.createElement('textarea');
                  ta.value = text;
                  ta.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;';
                  document.body.appendChild(ta);
                  ta.focus(); ta.select();
                  const ok = document.execCommand('copy');
                  document.body.removeChild(ta);
                  resolve(ok);
                });
            } else {
              // Method 2 directly
              const ta = document.createElement('textarea');
              ta.value = text;
              ta.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;';
              document.body.appendChild(ta);
              ta.focus(); ta.select();
              const ok = document.execCommand('copy');
              document.body.removeChild(ta);
              resolve(ok);
            }
          });
        });
      },
    });

    copied = results[0]?.result === true;
  } catch (err) {
    copied = false;
  }

  // Always clean up after — whether it worked or not
  await chrome.storage.local.remove('__grabber_pending_copy__');

  if (copied) {
    grabBtn.className = 'grab-btn success';
    grabBtn.textContent = '✓ Copied!';
    setStatus(`${sizeKB} KB copied to clipboard`, 'ok');
  } else {
    // Final fallback — download as file
    downloadHTML(html, currentTab?.url || 'page');
    grabBtn.className = 'grab-btn success';
    grabBtn.textContent = '✓ Saved!';
    setStatus(`${sizeKB} KB — saved as file (clipboard blocked)`, 'ok');
  }

  setTimeout(() => {
    grabBtn.className = 'grab-btn';
    grabBtn.textContent = currentMode === 'pick' ? '🎯 Start Picking' : '⚡ Copy HTML';
    setStatus('');
  }, 2500);
}

// ═══════════════════════════════════════════════════════════════════
//  SMART AUTO-DETECT
// ═══════════════════════════════════════════════════════════════════
async function runAutoDetect() {
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId: currentTab.id },
      func: detectChatLayout,
    });
    const found = results[0]?.result;
    if (!found || !found.length) return;

    detectBanner.style.display = 'block';
    detectDesc.textContent = `Found ${found.length} section(s) on this page:`;
    detectSections.innerHTML = '';
    found.forEach(item => {
      const tag = document.createElement('div');
      tag.className = 'detect-tag';
      tag.textContent = item.label;
      tag.title = item.selector;
      tag.addEventListener('click', async () => {
        selectorInput.value = item.selector;
        pills.forEach(p => p.classList.remove('active'));
        document.querySelector('[data-mode="selector"]').classList.add('active');
        currentMode = 'selector';
        selectorRow.style.display = 'block';
        grabBtn.textContent = '⚡ Copy HTML';
        grabBtn.className = 'grab-btn';
        setStatus(`Selector set: ${item.selector}`, 'ok');
      });
      detectSections.appendChild(tag);
    });
  } catch(_) {}
}

// Injected into page — detects common chat layout patterns
function detectChatLayout() {
  const found = [];

  // Generic chat/message table
  const tables = document.querySelectorAll('table');
  tables.forEach((t, i) => {
    const rows = t.querySelectorAll('tr');
    if (rows.length > 2) {
      const id = t.id ? `#${t.id}` : (t.className ? `.${t.className.trim().split(' ')[0]}` : `table:nth-of-type(${i+1})`);
      found.push({ label: `Table (${rows.length} rows)`, selector: id });
    }
  });

  // Divs with background color hints (blue/gray = chat panels)
  const allDivs = document.querySelectorAll('div[style]');
  allDivs.forEach((d, i) => {
    const style = d.getAttribute('style') || '';
    if (style.includes('rgb(204, 204, 255)')) found.push({ label: '🟣 Customer panel', selector: `[style*="rgb(204, 204, 255)"]` });
    if (style.includes('rgb(255, 204, 204)')) found.push({ label: '🔴 Persona panel',  selector: `[style*="rgb(255, 204, 204)"]` });
    if (style.includes('rgb(223, 233, 246)')) found.push({ label: '🔵 Details panel',  selector: `[style*="rgb(223, 233, 246)"]` });
  });

  // Deduplicate by selector
  const seen = new Set();
  return found.filter(f => { if (seen.has(f.selector)) return false; seen.add(f.selector); return true; }).slice(0, 6);
}

// ═══════════════════════════════════════════════════════════════════
//  ELEMENT PICKER (injected into page)
// ═══════════════════════════════════════════════════════════════════
function injectPicker() {
  if (document.getElementById('__htmlgrabber_overlay__')) return;
  let hovered = null;

  const banner = document.createElement('div');
  banner.id = '__htmlgrabber_overlay__';
  banner.style.cssText = `position:fixed;top:0;left:0;right:0;background:rgba(0,255,136,0.92);color:#0d0d0d;font:700 13px monospace;text-align:center;padding:8px;z-index:2147483647;letter-spacing:1px;box-shadow:0 2px 12px rgba(0,0,0,.4);`;
  banner.textContent = '🎯 HTML GRABBER — hover & click any element  |  ESC to cancel';
  document.body.appendChild(banner);

  const hl = document.createElement('div');
  hl.style.cssText = `position:fixed;pointer-events:none;z-index:2147483646;border:2px solid #00ff88;background:rgba(0,255,136,.08);border-radius:3px;transition:all .08s ease;box-shadow:0 0 0 1px rgba(0,255,136,.3);`;
  document.body.appendChild(hl);

  const label = document.createElement('div');
  label.style.cssText = `position:fixed;z-index:2147483647;background:#0d0d0d;color:#00ff88;font:700 10px monospace;padding:3px 7px;border-radius:3px;border:1px solid #00ff88;pointer-events:none;display:none;`;
  document.body.appendChild(label);

  function getXPath(el) {
    if (el.id) return `//*[@id="${el.id}"]`;
    const parts = [];
    let node = el;
    while (node && node.nodeType === 1) {
      let idx = 1, sib = node.previousSibling;
      while (sib) { if (sib.nodeType === 1 && sib.tagName === node.tagName) idx++; sib = sib.previousSibling; }
      parts.unshift(`${node.tagName.toLowerCase()}[${idx}]`);
      node = node.parentNode;
    }
    return '/' + parts.join('/');
  }

  function onMove(e) {
    const el = document.elementFromPoint(e.clientX, e.clientY);
    if (!el || el === banner || el === hl || el === label) return;
    hovered = el;
    const r = el.getBoundingClientRect();
    Object.assign(hl.style, { top: r.top+'px', left: r.left+'px', width: r.width+'px', height: r.height+'px' });
    label.style.display = 'block';
    label.style.top  = Math.max(0, r.top - 22) + 'px';
    label.style.left = r.left + 'px';
    label.textContent = `<${el.tagName.toLowerCase()}> ${el.id ? '#'+el.id : ''}${el.className && typeof el.className === 'string' ? '.'+el.className.trim().split(' ')[0] : ''}`;
  }

  function onClick(e) {
    if (!hovered || hovered === banner) return;
    e.preventDefault(); e.stopPropagation();

    // Serialize using same proper HTML format as full-page grab
    const SKIP_TAGS = new Set(['script','style','noscript','meta','link','head','template','slot']);
    const VOID_TAGS = new Set(['area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr']);
    const STYLE_PROPS = ['display','overflow','overflow-x','overflow-y','flex-direction','flex-wrap','flex','flex-grow','flex-shrink','justify-content','align-items','align-self','gap','position','top','right','bottom','left','z-index','width','height','min-width','max-width','min-height','max-height','padding','padding-top','padding-right','padding-bottom','padding-left','margin','margin-top','margin-right','margin-bottom','margin-left','border','border-top','border-right','border-bottom','border-left','border-radius','border-collapse','border-color','border-width','background-color','background-image','background-size','background-position','color','font-family','font-size','font-weight','font-style','line-height','letter-spacing','text-align','text-decoration','text-transform','white-space','word-break','vertical-align','cursor','opacity','box-sizing','user-select','resize','list-style','transform','aspect-ratio'];
    const SKIP_VALUES = new Set(['','auto','normal','none','initial','unset','inherit','static','inline','0px','0%','0','rgba(0, 0, 0, 0)','transparent','visible','start','left','top','separate','disc','outside']);
    const HTML_ATTRS = ['id','class','href','src','srcset','alt','placeholder','rows','cols','type','name','width','height','loading','decoding','data-nimg','role','aria-label'];

    function buildStyle(el) {
      const c = window.getComputedStyle(el), orig = el.getAttribute('style') || '';
      const map = new Map();
      STYLE_PROPS.forEach(p => { const v = c.getPropertyValue(p).trim(); if (v && !SKIP_VALUES.has(v)) map.set(p, v); });
      if (orig) orig.split(';').forEach(d => { const i=d.indexOf(':'); if(i===-1)return; const k=d.slice(0,i).trim(),v=d.slice(i+1).trim(); if(k&&v)map.set(k,v); });
      return Array.from(map.entries()).map(([k,v])=>`${k}:${v}`).join(';');
    }
    function esc(s){return s.replace(/&/g,'&amp;').replace(/"/g,'&quot;');}
    function escT(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
    function ser(node, depth) {
      const ind = '    '.repeat(depth);
      if (node.nodeType===3){const t=node.textContent.replace(/\s+/g,' ').trim();return t?ind+escT(t)+'\n':'';}
      if (node.nodeType!==1)return '';
      const tag=node.tagName.toLowerCase(); if(SKIP_TAGS.has(tag))return '';
      const ap=[];
      const s=buildStyle(node); if(s)ap.push(`style="${esc(s)}"`);
      HTML_ATTRS.forEach(a=>{const v=node.getAttribute(a);if(v!==null&&v.trim())ap.push(`${a}="${esc(v.trim())}"`);});
      // Live form values — JS-set .value on inputs/textareas/selects doesn't
      // always touch the DOM attribute or child text, so read it directly.
      if (tag==='input') {
        const type=(node.getAttribute('type')||'text').toLowerCase();
        if ((type==='checkbox'||type==='radio') && node.checked) ap.push('checked="checked"');
        if (node.value!=='') ap.push(`value="${esc(node.value)}"`);
      } else if (tag==='select') {
        const opt=node.options[node.selectedIndex];
        if (opt) { const sel=(opt.value||opt.textContent).trim(); if (sel) ap.push(`data-selected-value="${esc(sel)}"`); }
      } else if (tag==='option') {
        if (node.selected) ap.push('selected="selected"');
        if (node.value!=='') ap.push(`value="${esc(node.value)}"`);
      }
      const as=ap.length?' '+ap.join(' '):'';
      if(VOID_TAGS.has(tag))return `${ind}<${tag}${as} />\n`;
      if(tag==='textarea')return `${ind}<${tag}${as}>${escT(node.value||'')}</${tag}>\n`;
      const kids=Array.from(node.childNodes);
      const textOnly=kids.every(c=>c.nodeType===3||(c.nodeType===1&&SKIP_TAGS.has(c.tagName.toLowerCase())));
      const txt=node.textContent.replace(/\s+/g,' ').trim();
      if(textOnly&&txt)return `${ind}<${tag}${as}>${escT(txt)}</${tag}>\n`;
      let out=`${ind}<${tag}${as}>\n`;
      for(const ch of kids)out+=ser(ch,depth+1);
      return out+`${ind}</${tag}>\n`;
    }

    const tree = ser(hovered, 0);
    const result = { html: tree, xpath: getXPath(hovered) };
    cleanup();
    sessionStorage.setItem('__htmlgrabber_picked__', JSON.stringify(result));
  }

  function onKey(e) {
    if (e.key === 'Escape') { cleanup(); sessionStorage.setItem('__htmlgrabber_cancelled__', '1'); }
  }

  function cleanup() {
    document.removeEventListener('mousemove', onMove, true);
    document.removeEventListener('click', onClick, true);
    document.removeEventListener('keydown', onKey, true);
    banner.remove(); hl.remove(); label.remove();
  }

  document.addEventListener('mousemove', onMove, true);
  document.addEventListener('click', onClick, true);
  document.addEventListener('keydown', onKey, true);
}

// ═══════════════════════════════════════════════════════════════════
//  HISTORY
// ═══════════════════════════════════════════════════════════════════
async function loadHistory() {
  const data = await chrome.storage.local.get('grab_history');
  return data.grab_history || [];
}

async function addToHistory(item) {
  let history = await loadHistory();
  // Store metadata only — NOT the full HTML (can be MBs, blows storage quota fast)
  history.unshift({
    mode:   item.mode,
    url:    item.url,
    sizeKB: item.sizeKB,
    xpath:  item.xpath || null,
    ts:     item.ts,
    // store a short preview snippet only (first 300 chars)
    preview: item.html ? item.html.slice(0, 300) : '',
  });
  if (history.length > 10) history = history.slice(0, 10);
  await chrome.storage.local.set({ grab_history: history });
}

async function renderHistory() {
  const history = await loadHistory();
  if (!history.length) {
    historyEmpty.style.display = 'block';
    historyList.style.display  = 'none';
    historyClear.style.display = 'none';
    return;
  }
  historyEmpty.style.display = 'none';
  historyList.style.display  = 'flex';
  historyClear.style.display = 'block';
  historyList.innerHTML = '';

  history.forEach((item, i) => {
    const div = document.createElement('div');
    div.className = 'h-item';
    const date = new Date(item.ts);
    const timeStr = date.toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' }) + ' ' + date.toLocaleDateString();
    div.innerHTML = `
      <div class="h-item-top">
        <div class="h-item-mode">${item.mode}</div>
        <div class="h-item-size">${item.sizeKB} KB</div>
      </div>
      <div class="h-item-url">${item.url}</div>
      <div class="h-item-time">${timeStr}</div>
      <div class="h-item-actions">
        <div class="h-action" data-idx="${i}" data-action="copy">📋 Copy</div>
        <div class="h-action" data-idx="${i}" data-action="save">💾 Save</div>
        ${item.xpath ? `<div class="h-action" data-idx="${i}" data-action="xpath">📍 XPath</div>` : ''}
        <div class="h-action red" data-idx="${i}" data-action="delete">✕</div>
      </div>`;
    historyList.appendChild(div);
  });

  // Action handlers
  historyList.querySelectorAll('.h-action').forEach(btn => {
    btn.addEventListener('click', async () => {
      const history = await loadHistory();
      const idx = parseInt(btn.dataset.idx);
      const item = history[idx];
      const action = btn.dataset.action;
      if (action === 'copy') {
        // Only the most recent grab (idx=0) can be copied — it's still in memory
        if (idx === 0 && lastGrabbedHTML) {
          await navigator.clipboard.writeText(lastGrabbedHTML);
          btn.textContent = '✓ Copied'; setTimeout(() => btn.textContent = '📋 Copy', 1500);
        } else {
          btn.textContent = '⚠ Re-grab needed'; setTimeout(() => btn.textContent = '📋 Copy', 2000);
        }
      } else if (action === 'save') {
        if (idx === 0 && lastGrabbedHTML) {
          downloadHTML(lastGrabbedHTML, item.url);
          btn.textContent = '✓ Saved'; setTimeout(() => btn.textContent = '💾 Save', 1500);
        } else {
          btn.textContent = '⚠ Re-grab needed'; setTimeout(() => btn.textContent = '💾 Save', 2000);
        }
      } else if (action === 'xpath') {
        await navigator.clipboard.writeText(item.xpath);
        btn.textContent = '✓ Copied'; setTimeout(() => btn.textContent = '📍 XPath', 1500);
      } else if (action === 'delete') {
        history.splice(idx, 1);
        await chrome.storage.local.set({ grab_history: history });
        renderHistory();
      }
    });
  });
}

historyClear.addEventListener('click', async () => {
  await chrome.storage.local.set({ grab_history: [] });
  renderHistory();
});

// ═══════════════════════════════════════════════════════════════════
//  DIFF
// ═══════════════════════════════════════════════════════════════════
async function renderDiffSelects() {
  const history = await loadHistory();
  [diffA, diffB].forEach(sel => {
    sel.innerHTML = '<option value="">— Select grab —</option>';
    history.forEach((item, i) => {
      const date = new Date(item.ts);
      const label = `${item.mode} · ${item.sizeKB}KB · ${date.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}`;
      sel.innerHTML += `<option value="${i}">${label}</option>`;
    });
  });
}

diffBtn.addEventListener('click', async () => {
  const history = await loadHistory();
  const idxA = diffA.value, idxB = diffB.value;
  if (idxA === '' || idxB === '') { diffResult.innerHTML = '<div class="diff-empty">Select both grabs first.</div>'; return; }
  if (idxA === idxB) { diffResult.innerHTML = '<div class="diff-empty">Select two different grabs.</div>'; return; }

  const htmlA = history[idxA].html;
  const htmlB = history[idxB].html;

  // Simple line-by-line diff
  const linesA = htmlA.split('\n').map(l => l.trim()).filter(Boolean);
  const linesB = htmlB.split('\n').map(l => l.trim()).filter(Boolean);
  const setA = new Set(linesA);
  const setB = new Set(linesB);

  const added   = linesB.filter(l => !setA.has(l)).slice(0, 30);
  const removed = linesA.filter(l => !setB.has(l)).slice(0, 30);

  let html = '';
  if (!added.length && !removed.length) {
    html = '<div class="diff-empty">✓ No differences found.</div>';
  } else {
    if (removed.length) {
      html += `<div class="diff-removed" style="margin-bottom:6px;font-weight:700;">— Removed (${removed.length} lines)</div>`;
      removed.slice(0, 15).forEach(l => {
        html += `<div class="diff-removed">- ${escHtml(l.slice(0,80))}${l.length>80?'…':''}</div>`;
      });
    }
    if (added.length) {
      html += `<div class="diff-added" style="margin:8px 0 4px;font-weight:700;">+ Added (${added.length} lines)</div>`;
      added.slice(0, 15).forEach(l => {
        html += `<div class="diff-added">+ ${escHtml(l.slice(0,80))}${l.length>80?'…':''}</div>`;
      });
    }
  }
  diffResult.innerHTML = html;
});

// ═══════════════════════════════════════════════════════════════════
//  SETTINGS
// ═══════════════════════════════════════════════════════════════════
async function loadSettings() {
  const data = await chrome.storage.local.get('settings');
  if (data.settings) settings = { ...settings, ...data.settings };
  document.getElementById('settingAutoDetect').checked  = settings.autoDetect;
  document.getElementById('settingHistory').checked     = settings.saveHistory;
  document.getElementById('settingXpath').checked       = settings.autoXpath;
  document.getElementById('settingTimestamp').checked   = settings.timestampFiles;
}

['settingAutoDetect','settingHistory','settingXpath','settingTimestamp'].forEach(id => {
  document.getElementById(id).addEventListener('change', async (e) => {
    const map = { settingAutoDetect:'autoDetect', settingHistory:'saveHistory', settingXpath:'autoXpath', settingTimestamp:'timestampFiles' };
    settings[map[id]] = e.target.checked;
    await chrome.storage.local.set({ settings });
    if (id === 'settingAutoDetect') {
      if (settings.autoDetect) runAutoDetect();
      else detectBanner.style.display = 'none';
    }
  });
});

// ═══════════════════════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════════════════════
function downloadHTML(html, url) {
  const safeName = url.replace(/https?:\/\//, '').replace(/[^a-z0-9]/gi, '_').slice(0, 60);
  const ts = settings.timestampFiles ? '_' + new Date().toISOString().slice(0,19).replace(/:/g,'-') : '';
  const filename = `${safeName}${ts}.html`;
  const blob = new Blob([html], { type: 'text/html' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

function showError(msg) {
  grabBtn.className = 'grab-btn error';
  grabBtn.textContent = '✗ Failed';
  setStatus(msg, 'err');
  setTimeout(() => {
    grabBtn.className = 'grab-btn';
    grabBtn.textContent = currentMode === 'pick' ? '🎯 Start Picking' : '⚡ Copy HTML';
    setStatus('');
  }, 3000);
}

function setLoading(label) { grabBtn.className = 'grab-btn loading'; grabBtn.textContent = label; setStatus(''); }
function setStatus(msg, type = '') { status.textContent = msg; status.className = 'status' + (type ? ' '+type : ''); }
function flashBtn(btn, label) { const orig = btn.textContent; btn.textContent = label; setTimeout(() => btn.textContent = orig, 1800); }
function escHtml(str) { return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// ═══════════════════════════════════════════════════════════════════
//  LIVE STATS TAB — reads the operator dashboard's own "statistics"
//  API call (the same one triggered by clicking "Meine Statistiken")
// ═══════════════════════════════════════════════════════════════════
const STATS_HOSTS = [
  'mods.diamondchat.net',
  'mods.chatsx.net',
  'mods.gold-chat.net',
  'mods.platin-chat.com',
  'mods.mltestapp.com',
];

const statsSiteInfo = document.getElementById('statsSiteInfo');
const statsBtn      = document.getElementById('statsBtn');
const statsStatus   = document.getElementById('statsStatus');
const statsEmpty    = document.getElementById('statsEmpty');
const statsGrid     = document.getElementById('statsGrid');
const statsTime     = document.getElementById('statsTime');

function initStatsTab() {
  const host = currentTab?.url ? safeHostname(currentTab.url) : '';
  statsSiteInfo.textContent = host || '—';
  const ok = STATS_HOSTS.includes(host);
  statsBtn.disabled = !ok;
  statsStatus.textContent = ok ? '' : 'Open one of the 5 supported chat mod sites (and log in) first.';
  statsStatus.className = 'status' + (ok ? '' : ' warn');
}

function safeHostname(url) {
  try { return new URL(url).hostname; } catch (_) { return ''; }
}

statsBtn?.addEventListener('click', async () => {
  statsStatus.textContent = '⏳ Reading Meine Statistiken API call…';
  statsStatus.className = 'status warn';
  statsBtn.disabled = true;
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId: currentTab.id },
      world: 'MAIN',
      func: grabLiveStatsFromPage,
    });
    const stats = results[0]?.result;
    if (!stats) {
      statsStatus.textContent = '⚠ No stats response detected. Open the chat, click "Meine Statistiken" once yourself, then retry.';
      statsStatus.className = 'status err';
      return;
    }
    renderStats(stats);
    statsStatus.textContent = '✓ Stats updated from live API call';
    statsStatus.className = 'status ok';
    statsTime.textContent = new Date().toLocaleTimeString();
  } catch (e) {
    statsStatus.textContent = 'Error: ' + e.message;
    statsStatus.className = 'status err';
  } finally {
    statsBtn.disabled = false;
  }
});

function renderStats(stats) {
  const fields = [
    { key: 'ins',            label: 'Ins',            hi: true },
    { key: 'asaOuts',        label: 'ASA Outs',       hi: true },
    { key: 'outs',           label: 'Outs' },
    { key: 'openIns',        label: 'Open Ins' },
    { key: 'openOuts',       label: 'Open Outs' },
    { key: 'asaIns',         label: 'ASA Ins' },
    { key: 'durationTimeOut',label: 'Timeout (s)' },
  ];
  statsGrid.innerHTML = fields
    .filter(f => stats[f.key] !== undefined && stats[f.key] !== null)
    .map(f => `<div class="stat-card ${f.hi ? 'hi' : ''}"><div class="stat-label">${escHtml(f.label)}</div><div class="stat-value">${escHtml(String(stats[f.key]))}</div></div>`)
    .join('');
  statsGrid.style.display = 'grid';
  statsEmpty.style.display = 'none';
}

// Injected into the page's MAIN world. Temporarily hooks fetch + XHR to
// catch the JSON response of the site's own "statistics" API call, and
// (if the stats panel isn't already open) clicks the "Meine Statistiken"
// button itself to trigger that call. Restores everything afterward.
function grabLiveStatsFromPage() {
  const REQUIRED_KEYS = ['ins', 'outs', 'asaOuts', 'asaIns', 'openIns', 'openOuts'];

  function extractStats(json) {
    if (!json || typeof json !== 'object') return null;
    const src = json.data && typeof json.data === 'object' ? json.data : json;
    let hits = 0;
    for (const k of REQUIRED_KEYS) if (Object.prototype.hasOwnProperty.call(src, k)) hits++;
    return hits >= 3 ? src : null;
  }

  return new Promise((resolve) => {
    let settled = false;
    const origFetch = window.fetch;
    const origOpen  = XMLHttpRequest.prototype.open;
    const origSend  = XMLHttpRequest.prototype.send;

    function finish(result) {
      if (settled) return;
      settled = true;
      window.fetch = origFetch;
      XMLHttpRequest.prototype.open = origOpen;
      XMLHttpRequest.prototype.send = origSend;
      resolve(result);
    }

    window.fetch = async function (...args) {
      const res = await origFetch.apply(this, args);
      try {
        res.clone().json().then((json) => {
          const stats = extractStats(json);
          if (stats) finish(stats);
        }).catch(() => {});
      } catch (_) {}
      return res;
    };

    XMLHttpRequest.prototype.open = function (...args) {
      return origOpen.apply(this, args);
    };
    XMLHttpRequest.prototype.send = function (...args) {
      this.addEventListener('load', function () {
        try {
          const json = JSON.parse(this.responseText);
          const stats = extractStats(json);
          if (stats) finish(stats);
        } catch (_) {}
      });
      return origSend.apply(this, args);
    };

    // Try to fire the request ourselves by clicking the site's own
    // "Meine Statistiken" button, so the user doesn't have to.
    try {
      const candidates = Array.from(document.querySelectorAll('button, a, div, span'));
      const btn = candidates.find((el) => {
        const t = (el.textContent || '').trim();
        return /Statistik|Statistics/i.test(t) && t.length < 40 && el.children.length <= 2;
      });
      if (btn) btn.click();
    } catch (_) {}

    setTimeout(() => finish(null), 6000);
  });
}

// ── Boot ──────────────────────────────────────────────────────────
init();
