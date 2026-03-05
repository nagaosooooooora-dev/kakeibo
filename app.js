console.log("app.js loaded");

import { firebaseConfig } from "./firebase-config.js?v=1";
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js";
import { getFirestore, doc, getDoc, setDoc, onSnapshot, serverTimestamp, enableIndexedDbPersistence } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js";
import { getAuth, GoogleAuthProvider, signInWithPopup, signInWithRedirect, getRedirectResult, signOut, onAuthStateChanged, setPersistence, browserLocalPersistence } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js";
import {
  getAuth,
  onAuthStateChanged,
  GoogleAuthProvider,
  signInWithPopup,
  signInWithRedirect,
  getRedirectResult,
  setPersistence,
  browserLocalPersistence,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";

// Budget App Prototype (localStorage)
// v0.6 — All missing features in one pass:
// - 取引入力（サブカテゴリ/イレギュラー/メモ）
// - 実績は取引から自動集計（表の直接入力は「調整」取引）
// - 予算マスター編集 + 月予算スナップショット（締め月は凍結）
// - 締め解除（差し戻し）
// - AI改善点（簡易ヒューリスティック）

const CATEGORIES = [
  "住宅","水道・光熱費","通信費","自動車","保険",
  "健康・医療","税・社会保障","食費","日用品",
  "教養・教育","趣味・娯楽","衣服・美容",
  "その他","交通費","交際費"
];

const CATEGORY_GROUPS = [
  { name: "固定費", cats: ["住宅","水道・光熱費","通信費","自動車","保険","税・社会保障"] },
  { name: "生活", cats: ["食費","日用品","健康・医療","交通費"] },
  { name: "自己投資/趣味", cats: ["教養・教育","趣味・娯楽","衣服・美容","交際費","その他"] },
];

const DEFAULT_BUDGET = {
  "住宅": 80000,
  "水道・光熱費": 15000,
  "通信費": 10000,
  "自動車": 15000,
  "保険": 10000,
  "健康・医療": 5000,
  "税・社会保障": 10000,
  "食費": 40000,
  "日用品": 8000,
  "教養・教育": 5000,
  "趣味・娯楽": 20000,
  "衣服・美容": 10000,
  "その他": 5000,
  "交通費": 10000,
  "交際費": 20000
};

const STORAGE_KEY = "mfBudgetProto_v10";

let state = {
  config: {
    budgetMaster: deepClone(DEFAULT_BUDGET),
    ui: { showTxPanel: false }
  },
  months: {
    // [YYYY-MM]: {
    //   closed:boolean,
    //   budgetSnapshot:{cat:number},
    //   tx:[{id,date,cat,subcat,amount,irregular,memo,kind:"normal"|"manual"}],
    //   closeMeta:{ romanceDeposit, debtAdded:{cat:amt}, forcedRepay:{toCat,amount}, repayments:[{from,to,amount}] }
    // }
  },
  debt: {},
  romance: { balance: 0, history: [] } // history: [{month, amount}]
};

// --- helpers ---
function deepClone(obj){ return JSON.parse(JSON.stringify(obj)); }
function yen(n){
  const x = Number(n || 0);
  return x.toLocaleString("ja-JP");
}
function clampInt(n, min, max){
  const v = Number(n);
  if (!Number.isFinite(v)) return min;
  return Math.min(max, Math.max(min, Math.floor(v)));
}
function uuid(){
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}
function setSyncChip(text){
  const el = document.getElementById("syncChip");
  if (el) el.textContent = text;
}

function todayISO(){
  const d = new Date();
  const y=d.getFullYear();
  const m=String(d.getMonth()+1).padStart(2,'0');
  const day=String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}
function monthFromDateISO(dateISO){
  return dateISO.slice(0,7);
}

// --- firebase setup ---
const fbApp = initializeApp(firebaseConfig);
const db = getFirestore(fbApp);
const auth = getAuth(fbApp);
// iPhone Safariでログイン状態を保持する
await setPersistence(auth, browserLocalPersistence);
// Keep session across reloads (important on iOS Safari)
setPersistence(auth, browserLocalPersistence).catch(()=>{});
const provider = new GoogleAuthProvider();

let uid = null;
let stateDocRef = null;

// Offline persistence (best-effort)
enableIndexedDbPersistence(db).catch(()=>{});

// Stable client id (per-browser) to avoid echo loops
const CLIENT_ID_KEY = STORAGE_KEY + "::clientId";
let clientId = localStorage.getItem(CLIENT_ID_KEY);
if (!clientId) {
  clientId = Math.random().toString(16).slice(2) + Date.now().toString(16);
  localStorage.setItem(CLIENT_ID_KEY, clientId);
}

// Debounced save
let saveTimer = null;
let localRev = 0; // increments on each local write
let applyingRemote = false;

// --- state io (Firestore) ---
function normalizeState(){
  if (!state.config) state.config = { budgetMaster: deepClone(DEFAULT_BUDGET), ui:{showTxPanel:false} };
  if (!state.config.budgetMaster) state.config.budgetMaster = deepClone(DEFAULT_BUDGET);
  if (!state.config.ui) state.config.ui = { showTxPanel:false };
  if (typeof state.config.ui.showTxPanel !== "boolean") state.config.ui.showTxPanel = false;
  if (!state.months) state.months = {};
  if (!state.debt) state.debt = {};
  if (!state.romance) state.romance = { balance: 0, history: [] };
  if (!Array.isArray(state.romance.history)) state.romance.history = [];

  CATEGORIES.forEach(c => {
    if (typeof state.debt[c] !== "number") state.debt[c] = 0;
    if (typeof state.config.budgetMaster[c] !== "number") state.config.budgetMaster[c] = DEFAULT_BUDGET[c] || 0;
  });

  if (!state.meta) state.meta = { rev: 0 };
  if (typeof state.meta.rev !== "number") state.meta.rev = 0;
  localRev = state.meta.rev;
}

async function loadFromRemote(){
  if (!stateDocRef) return;
  setSyncChip("同期: 読み込み…");
  const snap = await getDoc(stateDocRef);
  if (snap.exists()){
    const data = snap.data();
    if (data && data.state){
      applyingRemote = true;
      state = data.state;
      normalizeState();
      applyingRemote = false;
      setSyncChip("同期: OK");
      render();
    }
  } else {
    normalizeState();
    await writeRemoteNow("init");
    setSyncChip("同期: OK");
    render();
  }

  onSnapshot(stateDocRef, (docSnap)=>{
    if (!docSnap.exists()) return;
    const data = docSnap.data();
    if (!data || !data.state) return;

    const remoteRev = data.state?.meta?.rev ?? 0;
    const remoteClient = data.meta?.clientId;

    if (remoteClient === clientId && remoteRev <= localRev) return;

    if (remoteRev > localRev){
      applyingRemote = true;
      state = data.state;
      normalizeState();
      applyingRemote = false;
      setSyncChip("同期: 更新あり");
      render();
      setTimeout(()=>setSyncChip("同期: OK"), 700);
    }
  });
}

function scheduleSave(){
  if (applyingRemote) return;
  if (!uid || !stateDocRef) return;

  setSyncChip(navigator.onLine ? "同期: 送信待ち" : "同期: オフライン（キュー）");

  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(()=>{
    writeRemoteNow("debounced");
  }, 500);
}

async function writeRemoteNow(reason){
  if (!uid || !stateDocRef) return;
  try{
    state.meta = state.meta || {};
    state.meta.rev = (state.meta.rev || 0) + 1;
    localRev = state.meta.rev;

    await setDoc(stateDocRef, {
      state,
      meta: { clientId, updatedAt: serverTimestamp(), reason }
    }, { merge: false });

    setSyncChip("同期: OK");
  } catch (e){
    console.error(e);
    setSyncChip("同期: エラー");
  }
}


function ensureMonth(month) {
  if (!state.months[month]) {
    state.months[month] = {
      closed: false,
      budgetSnapshot: deepClone(state.config.budgetMaster),
      tx: [],
      closeMeta: null
    };
  } else {
    const m = state.months[month];
    if (!m.budgetSnapshot) m.budgetSnapshot = deepClone(state.config.budgetMaster);
    if (!Array.isArray(m.tx)) m.tx = [];
  }
}

function getSelectedMonth(){
  return document.getElementById("monthPicker").value;
}

// --- compute ---
function budgetForMonth(month){
  const m = state.months[month];
  return m?.budgetSnapshot || state.config.budgetMaster;
}

function actualByCat(month){
  const m = state.months[month];
  const sums = {};
  CATEGORIES.forEach(c=>sums[c]=0);
  (m.tx || []).forEach(t=>{
    if (!CATEGORIES.includes(t.cat)) return;
    sums[t.cat] += Number(t.amount || 0);
  });
  return sums;
}

function computeMonthTotals(month){
  const bud = budgetForMonth(month);
  const act = actualByCat(month);
  let totalBudget=0, totalActual=0;
  CATEGORIES.forEach(c=>{
    totalBudget += Number(bud[c]||0);
    totalActual += Number(act[c]||0);
  });
  const delta = totalBudget - totalActual;
  return { totalBudget, totalActual, delta, bud, act };
}

function computeCategorySurplus(month){
  const bud = budgetForMonth(month);
  const act = actualByCat(month);
  const surplus = {};
  CATEGORIES.forEach(c=>{
    const s = (bud[c]||0) - (act[c]||0);
    surplus[c] = Math.max(0, s);
  });
  return surplus;
}

function deficitsByCat(month){
  const bud = budgetForMonth(month);
  const act = actualByCat(month);
  const def = {};
  CATEGORIES.forEach(c=>{
    const d = (act[c]||0) - (bud[c]||0);
    def[c] = Math.max(0, d);
  });
  return def;
}

function totalDebt(){
  return CATEGORIES.reduce((a,c)=>a+(state.debt[c]||0),0);
}

function txStats(month){
  const m = state.months[month];
  const subCounts = {}; // cat -> set size
  const irrSum = {}; // cat -> sum
  CATEGORIES.forEach(c=>{ subCounts[c]=new Set(); irrSum[c]=0; });
  (m.tx||[]).forEach(t=>{
    if (!CATEGORIES.includes(t.cat)) return;
    if (t.subcat) subCounts[t.cat].add(String(t.subcat).trim());
    if (t.irregular) irrSum[t.cat] += Number(t.amount||0);
  });
  const subCountNum = {}; CATEGORIES.forEach(c=>subCountNum[c]=subCounts[c].size);
  return { subCountNum, irrSum };
}

// --- rendering ---
function render(){
  const month = getSelectedMonth();
  if (!month) return;
  ensureMonth(month);
  renderKpis(month);
  renderMonthChips(month);
  renderTable(month);
  renderTxList(month);
  renderDebt();
  renderReport();
  renderAI(month);
  wireSegButtons(); // keep active state
  syncActionButtons(month);

function applyUiVisibility(){
  const showTx = !!state.config.ui?.showTxPanel;
  const txPanel = document.getElementById("txPanel");
  if (txPanel){
    txPanel.classList.toggle("hiddenPanel", !showTx);
  }
}

}

function renderMonthChips(month){
  const m = state.months[month];
  const status = m.closed ? "締め済み" : "未締め";
  document.getElementById("monthStatusChip").textContent = status;
  document.getElementById("budgetSourceChip").textContent = m.closed ? "予算: スナップショット（凍結）" : "予算: 今月スナップショット（編集可）";
}

function syncActionButtons(month){
  const m = state.months[month];
  document.getElementById("reopenMonthBtn").disabled = !m.closed;
  document.getElementById("closeMonthBtn").disabled = m.closed;
  document.getElementById("syncBudgetBtn").disabled = m.closed;
  document.getElementById("addTxBtn").disabled = m.closed;
}

function renderKpis(month){
  const m = state.months[month];
  const { totalBudget, totalActual, delta } = computeMonthTotals(month);
  const isDeficit = delta < 0;
  const statusText = isDeficit ? "赤字" : (delta===0 ? "トントン" : "黒字");
  const romancePlanned = isDeficit ? 5000 : 10000;
  const romanceShown = m.closed ? (m.closeMeta?.romanceDeposit||0) : romancePlanned;

  const irrTotal = (m.tx||[]).filter(t=>t.irregular).reduce((a,t)=>a+Number(t.amount||0),0);

  const kpis = [
    { label:"今月の差分", value:`${delta>=0?"+":""}${yen(delta)}`, chip:statusText, tone: isDeficit ? "bad" : (delta===0 ? "warn":"good") },
    { label:"実績 / 予算", value:`${yen(totalActual)} / ${yen(totalBudget)}`, chip:"月合計", tone:"" },
    { label:"ロマン残高", value: yen(state.romance.balance), chip: m.closed ? `今月+${yen(romanceShown)}` : `予定+${yen(romanceShown)}`, tone: state.romance.balance>0 ? "good":"" },
    { label:"借金総額", value: yen(totalDebt()), chip: irrTotal>0 ? `イレギュラー ${yen(irrTotal)}` : "全カテゴリ", tone: totalDebt()>0 ? "warn":"" }
  ];

  const wrap = document.getElementById("kpiCards");
  wrap.innerHTML = "";
  kpis.forEach(k=>{
    const div = document.createElement("div");
    div.className = `kpi ${k.tone||""}`;
    div.innerHTML = `
      <div class="kpiTop">
        <div class="kpiLabel">${k.label}</div>
        <div class="kpiChip">${k.chip}</div>
      </div>
      <div class="kpiValue">${k.value}</div>
    `;
    wrap.appendChild(div);
  });
}

function renderTable(month){
  const m = state.months[month];
  const tbody = document.querySelector("#budgetTable tbody");
  tbody.innerHTML = "";

  const bud = budgetForMonth(month);
  const act = actualByCat(month);
  const { subCountNum, irrSum } = txStats(month);

  CATEGORY_GROUPS.forEach(group=>{
    const gr = document.createElement("tr");
    gr.className = "groupRow";
    gr.innerHTML = `<td class="stickyCol" colspan="4">${group.name}</td>`;
    tbody.appendChild(gr);

    group.cats.forEach(cat=>{
      const budget = Number(bud[cat]||0);
      const actual = Number(act[cat]||0);
      const diff = budget - actual;
      const diffClass = diff >= 0 ? "good" : "bad";
      const rowTone = diff < 0 ? "deficit" : "surplus";
      const hasDebt = (state.debt[cat] || 0) > 0;
      const locked = m.closed;
      const subn = subCountNum[cat] || 0;
      const irr = irrSum[cat] || 0;

      const tr = document.createElement("tr");
      tr.className = `dataRow ${rowTone}`;

      tr.innerHTML = `
        <td class="stickyCol">
          <div class="catCell">
            <span>${cat}</span>
            ${hasDebt ? `<span class="badge debt">借金 ${yen(state.debt[cat])}</span>` : ``}
            ${subn>0 ? `<span class="badge sub">内訳 ${subn}</span>` : ``}
            ${irr>0 ? `<span class="badge irr">突発 ${yen(irr)}</span>` : ``}
            ${locked ? `<span class="badge lock">締め済</span>` : ``}
          </div>
        </td>
        <td class="num">${yen(budget)}</td>
        <td class="num">
          <input class="inline" type="number" inputmode="numeric" min="0" step="100" value="${actual}"
            data-cat="${cat}" ${locked ? "disabled" : ""} title="ここを変えると、調整取引として保存されます">
        </td>
        <td class="num diff ${diffClass}">${diff >= 0 ? "+" : ""}${yen(diff)}</td>
      `;
      tbody.appendChild(tr);
    });
  });
}

function renderDebt(){
  const div = document.getElementById("debtList");
  const entries = CATEGORIES.map(c => [c, state.debt[c] || 0]).sort((a,b)=>b[1]-a[1]);
  div.innerHTML = "";
  entries.forEach(([c,amt])=>{
    const el = document.createElement("div");
    el.className = "item";
    el.innerHTML = `<span class="name">${c}</span><span class="val">${yen(amt)}</span>`;
    div.appendChild(el);
  });
}

function closedMonthsSorted(){
  return Object.keys(state.months).filter(m => state.months[m]?.closed).sort();
}
function lastNMonths(arr, n){ return arr.length <= n ? arr : arr.slice(arr.length - n); }

function renderReport(){
  const div = document.getElementById("report");
  div.innerHTML = "";
  const closed = closedMonthsSorted();
  const last12 = lastNMonths(closed, 12);
  if (last12.length === 0) {
    div.innerHTML = `<div class="panelHint">まだ締めた月がありません。</div>`;
    return;
  }

  // Category totals across last12
  CATEGORIES.forEach(cat => {
    let budgetSum = 0, actualSum = 0;
    last12.forEach(month => {
      const bud = budgetForMonth(month);
      const act = actualByCat(month);
      budgetSum += (bud[cat] || 0);
      actualSum += (act[cat] || 0);
    });
    const diff = budgetSum - actualSum;
    const el = document.createElement("div");
    el.className = "item";
    el.innerHTML = `
      <span class="name">${cat}</span>
      <span class="val">差分 ${diff>=0?"+":""}${yen(diff)} / 借金 ${yen(state.debt[cat]||0)}</span>
    `;
    div.appendChild(el);
  });
}

// --- Tx list ---
let txFilter = "all"; // all | irregular | manual
function wireSegButtons(){
  document.querySelectorAll(".seg").forEach(btn=>{
    btn.classList.toggle("active", btn.dataset.filter === txFilter);
  });
}
function txMatchesFilter(t){
  if (txFilter === "irregular") return !!t.irregular;
  if (txFilter === "manual") return t.kind === "manual";
  return true;
}
function txMatchesSearch(t, q){
  if (!q) return true;
  const s = (t.memo||"") + " " + (t.cat||"") + " " + (t.subcat||"");
  return s.toLowerCase().includes(q.toLowerCase());
}
function renderTxList(month){
  const m = state.months[month];
  const div = document.getElementById("txList");
  div.innerHTML = "";
  const q = document.getElementById("txSearch").value || "";

  const tx = (m.tx||[])
    .filter(t=>txMatchesFilter(t))
    .filter(t=>txMatchesSearch(t,q))
    .slice()
    .sort((a,b)=> (a.date||"").localeCompare(b.date||"") || (a.id||"").localeCompare(b.id||""));

  if (tx.length === 0) {
    div.innerHTML = `<div class="panelHint">取引がありません。</div>`;
    return;
  }

  tx.forEach(t=>{
    const el = document.createElement("div");
    el.className = "item";
    const badges = [];
    if (t.subcat) badges.push(`<span class="badge sub">${t.subcat}</span>`);
    if (t.irregular) badges.push(`<span class="badge irr">突発</span>`);
    if (t.kind === "manual") badges.push(`<span class="badge lock">調整</span>`);
    if (t.kind === "bulk") badges.push(`<span class="badge lock">月次まとめ</span>`);
    el.innerHTML = `
      <div>
        <div class="name">${t.cat} <span class="val" style="margin-left:8px">${t.date || ""}</span></div>
        <div class="meta" style="margin-top:8px">${badges.join("")}</div>
        ${t.memo ? `<div class="val" style="margin-top:8px">${escapeHtml(t.memo)}</div>` : ``}
      </div>
      <div class="meta">
        <div class="val" style="font-weight:900">${yen(t.amount)}</div>
        <button class="itemBtn ghost" data-act="edit" data-id="${t.id}">編集</button>
        <button class="itemBtn danger" data-act="del" data-id="${t.id}">削除</button>
      </div>
    `;
    div.appendChild(el);
  });
}

function escapeHtml(s){
  return String(s)
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");
}

// --- Table direct input => manual adjustment tx ---
function upsertManualAdjustment(month, cat, desiredTotal){
  const m = state.months[month];
  const tx = m.tx || [];
  const existing = tx.find(t => t.kind === "manual" && t.cat === cat);

  // compute total of non-manual tx in this cat
  const base = tx.filter(t => t.cat === cat && t.kind !== "manual")
    .reduce((a,t)=>a+Number(t.amount||0),0);

  const adj = Math.max(0, Number(desiredTotal||0) - base);

  if (existing) {
    existing.amount = adj;
    existing.date = `${month}-01`;
    existing.memo = "調整（表入力）";
  } else {
    tx.push({
      id: "manual-" + cat,
      kind: "manual",
      date: `${month}-01`,
      cat,
      subcat: "",
      amount: adj,
      irregular: false,
      memo: "調整（表入力）"
    });
  }
  // If adj becomes 0, keep it (so the table stays consistent) — or we could delete it.
  m.tx = tx;
}

// --- Close / Reopen ---
const closeModal = {
  backdrop:null, closeX:null, cancel:null, confirm:null,
  closeSummary:null, forcedSection:null, forcedSelect:null,
  repayRows:null, addRow:null, validation:null, sub:null, visualPanel:null,
  currentMonth:null, workingDebt:null, workingSurplus:null, isDeficitMonth:false,
  workingBudget:null, workingActual:null
};

function openCloseModal(month){
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) { alert("既に締め済みです"); return; }

  const { totalBudget, totalActual, delta, bud, act } = computeMonthTotals(month);
  const isDeficitMonth = delta < 0;
  const romanceDeposit = isDeficitMonth ? 5000 : 10000;

  // workingDebt: current debt + deficits of this month
  const workingDebt = deepClone(state.debt);
  const def = deficitsByCat(month);
  CATEGORIES.forEach(c=>{
    if (def[c] > 0) workingDebt[c] = (workingDebt[c]||0) + def[c];
  });

  const surplus = computeCategorySurplus(month);

  closeModal.sub.textContent = `${month} / ロマン ${yen(romanceDeposit)} / ${isDeficitMonth ? "赤字" : (delta===0 ? "トントン":"黒字")}`;

  closeModal.closeSummary.innerHTML = `
    <div class="kpiTop" style="margin-bottom:8px">
      <div class="kpiLabel">月合計</div>
      <div class="kpiChip">${isDeficitMonth ? "赤字" : (delta===0 ? "トントン":"黒字")}</div>
    </div>
    <div class="kpiValue" style="font-size:20px">${delta>=0?"+":""}${yen(delta)}</div>
    <div class="small" style="margin-top:8px">予算 ${yen(totalBudget)} / 実績 ${yen(totalActual)} / ロマン積立（確定後） ${yen(romanceDeposit)}</div>
  `;

  closeModal.isDeficitMonth = isDeficitMonth;
  if (isDeficitMonth) {
    closeModal.forcedSection.classList.remove("hidden");
    const debtCats = CATEGORIES.filter(c => (workingDebt[c] || 0) > 0);
    closeModal.forcedSelect.innerHTML = "";
    (debtCats.length ? debtCats : CATEGORIES).forEach(c=>{
      const opt = document.createElement("option");
      opt.value = c;
      opt.textContent = debtCats.length ? `${c}（借金 ${yen(workingDebt[c])}）` : c;
      closeModal.forcedSelect.appendChild(opt);
    });
  } else {
    closeModal.forcedSection.classList.add("hidden");
    closeModal.forcedSelect.innerHTML = "";
  }

  closeModal.repayRows.innerHTML = "";
  closeModal.validation.textContent = "";
  addRepayRow();

  closeModal.currentMonth = month;
  closeModal.workingDebt = workingDebt;
  closeModal.workingSurplus = surplus;
  closeModal.workingBudget = bud;
  closeModal.workingActual = act;

  closeModal.backdrop.classList.remove("hidden");
  closeModal.backdrop.setAttribute("aria-hidden","false");
  validateRepayments();
}

function hideCloseModal(){
  closeModal.backdrop.classList.add("hidden");
  closeModal.backdrop.setAttribute("aria-hidden","true");
  closeModal.currentMonth = null;
}

function debtOptions(){
  return CATEGORIES.filter(c => (closeModal.workingDebt?.[c] || 0) > 0);
}
function surplusOptions(){
  return CATEGORIES.filter(c => (closeModal.workingSurplus?.[c] || 0) > 0);
}
function createSelect(options, placeholder, labelFn){
  const sel = document.createElement("select");
  const ph = document.createElement("option");
  ph.value = ""; ph.textContent = placeholder;
  sel.appendChild(ph);
  options.forEach(o=>{
    const opt = document.createElement("option");
    opt.value = o;
    opt.textContent = labelFn ? labelFn(o) : o;
    sel.appendChild(opt);
  });
  return sel;
}
function addRepayRow(){
  const wrap = document.createElement("div");
  wrap.className = "repayWrap";

  const row = document.createElement("div");
  row.className = "repayRow";

  const fromSel = createSelect(
    surplusOptions(),
    "返済元（余剰）",
    (c)=> `${c}（余剰 ${yen(closeModal.workingSurplus?.[c]||0)}）`
  );
  const toSel = createSelect(
    debtOptions(),
    "返済先（借金）",
    (c)=> `${c}（借金 ${yen(closeModal.workingDebt?.[c]||0)}）`
  );

  const amt = document.createElement("input");
  amt.type="number"; amt.min="0"; amt.step="100"; amt.inputMode="numeric"; amt.placeholder="金額";

  const delBtn = document.createElement("button");
  delBtn.className="btn ghost"; delBtn.textContent="削除";

  row.appendChild(fromSel);
  row.appendChild(toSel);
  row.appendChild(amt);
  row.appendChild(delBtn);

  const meta = document.createElement("div");
  meta.className="repayMeta";
  meta.innerHTML = `
    <div class="metaBox" data-kind="from">
      <div class="metaLine"><span>余剰</span><span><b class="cap">-</b> / 残り <b class="remain">-</b></span></div>
      <div class="barTrack"><div class="barFill" data-fill="from"></div></div>
    </div>
    <div class="metaBox" data-kind="to">
      <div class="metaLine"><span>借金</span><span><b class="cap">-</b> / 残り <b class="remain">-</b></span></div>
      <div class="barTrack"><div class="barFill warn" data-fill="to"></div></div>
    </div>
  `;

  wrap.appendChild(row);
  wrap.appendChild(meta);

  const onAnyChange = ()=>validateRepayments();
  fromSel.addEventListener("change", onAnyChange);
  toSel.addEventListener("change", onAnyChange);
  amt.addEventListener("input", onAnyChange);
  delBtn.addEventListener("click", ()=>{ wrap.remove(); validateRepayments(); });

  closeModal.repayRows.appendChild(wrap);
}

function parseRepayRows(){
  const rows = [];
  Array.from(closeModal.repayRows.querySelectorAll(".repayRow")).forEach(r=>{
    const sels = r.querySelectorAll("select");
    const inp = r.querySelector("input");
    rows.push({
      from: sels[0]?.value || "",
      to: sels[1]?.value || "",
      amount: clampInt(inp?.value || 0, 0, 10**12),
      _row: r
    });
  });
  return rows;
}

function validateRepayments(){
  const rows = parseRepayRows();
  rows.forEach(r=>{ r._row.style.outline="none"; });

  const msgs = [];
  const usedSurplus = {};
  const usedDebt = {};

  rows.forEach((r, idx)=>{
    if (!r.from && !r.to && r.amount === 0) return;
    if (!r.from || !r.to) {
      msgs.push(`返済${idx+1}: 返済元と返済先を選んでください。`);
      r._row.style.outline = "1px solid rgba(255,209,102,.7)";
      return;
    }
    if (r.amount <= 0) {
      msgs.push(`返済${idx+1}: 金額を入力してください。`);
      r._row.style.outline = "1px solid rgba(255,209,102,.7)";
      return;
    }
    usedSurplus[r.from] = (usedSurplus[r.from] || 0) + r.amount;
    usedDebt[r.to] = (usedDebt[r.to] || 0) + r.amount;
  });

  Object.entries(usedSurplus).forEach(([cat, used])=>{
    const cap = closeModal.workingSurplus?.[cat] || 0;
    if (used > cap) msgs.push(`返済元「${cat}」の余剰超過（余剰 ${yen(cap)} / 指定 ${yen(used)}）`);
  });
  Object.entries(usedDebt).forEach(([cat, used])=>{
    const cap = closeModal.workingDebt?.[cat] || 0;
    if (used > cap) msgs.push(`返済先「${cat}」の借金超過（借金 ${yen(cap)} / 指定 ${yen(used)}）`);
  });

  if (closeModal.isDeficitMonth) {
    const to = closeModal.forcedSelect.value;
    if (!to) msgs.push("強制返済の返済先を選んでください。");
  }

  if (surplusOptions().length === 0) msgs.unshift("この月はカテゴリ余剰がありません（任意返済なし）。");

  // per-row meta + visual panel
  rows.forEach((r)=>{
    const fromCap = r.from ? (closeModal.workingSurplus?.[r.from] || 0) : 0;
    const toCap = r.to ? (closeModal.workingDebt?.[r.to] || 0) : 0;
    const usedFrom = r.from ? (usedSurplus[r.from] || 0) : 0;
    const usedTo = r.to ? (usedDebt[r.to] || 0) : 0;
    const fromRemain = Math.max(0, fromCap - usedFrom);
    const toRemain = Math.max(0, toCap - usedTo);

    const metaFrom = r._row.parentElement.querySelector('[data-kind="from"]');
    const metaTo = r._row.parentElement.querySelector('[data-kind="to"]');

    if (metaFrom){
      metaFrom.querySelector('.cap').textContent = r.from ? yen(fromCap) : "-";
      metaFrom.querySelector('.remain').textContent = r.from ? yen(fromRemain) : "-";
      const fill = metaFrom.querySelector('[data-fill="from"]');
      const pct = fromCap > 0 ? Math.min(100, (usedFrom / fromCap) * 100) : 0;
      fill.style.width = pct.toFixed(0) + "%";
    }
    if (metaTo){
      metaTo.querySelector('.cap').textContent = r.to ? yen(toCap) : "-";
      metaTo.querySelector('.remain').textContent = r.to ? yen(toRemain) : "-";
      const fill = metaTo.querySelector('[data-fill="to"]');
      const pct = toCap > 0 ? Math.min(100, (usedTo / toCap) * 100) : 0;
      fill.style.width = pct.toFixed(0) + "%";
    }
  });

  const topSurplus = surplusOptions()
    .map(c=>({c, cap: closeModal.workingSurplus?.[c]||0, used: usedSurplus[c]||0}))
    .sort((a,b)=> (b.cap-b.used) - (a.cap-a.used))
    .slice(0,5);

  const topDebt = debtOptions()
    .map(c=>({c, cap: closeModal.workingDebt?.[c]||0, used: usedDebt[c]||0}))
    .sort((a,b)=> (b.cap-b.used) - (a.cap-a.used))
    .slice(0,5);

  if (closeModal.visualPanel){
    const makeBox = (title, items, kind) => {
      const box = document.createElement("div");
      box.className = "vbox";
      box.innerHTML = `<div class="vtitle">${title}</div>`;
      items.forEach(it=>{
        const remain = Math.max(0, it.cap - it.used);
        const row = document.createElement("div");
        row.className = "vrow";
        row.innerHTML = `<span class="vname">${it.c}</span><span class="vamt">${yen(remain)} / ${yen(it.cap)}</span>`;
        box.appendChild(row);
        const track = document.createElement("div");
        track.className="barTrack";
        const fill = document.createElement("div");
        fill.className = "barFill" + (kind==="debt" ? " warn" : "");
        const pct = it.cap>0 ? Math.min(100,(it.used/it.cap)*100) : 0;
        fill.style.width = pct.toFixed(0) + "%";
        track.appendChild(fill);
        box.appendChild(track);
      });
      return box;
    };
    closeModal.visualPanel.innerHTML="";
    closeModal.visualPanel.appendChild(makeBox("余剰（残り / 合計）", topSurplus, "surplus"));
    closeModal.visualPanel.appendChild(makeBox("借金（残り / 合計）", topDebt, "debt"));
  }

  closeModal.validation.textContent = msgs.join("\n");
  const hardErrors = msgs.filter(m => m.includes("選んでください") || m.includes("入力してください") || m.includes("超過"));
  closeModal.confirm.disabled = hardErrors.length > 0;

  // refresh select labels
  const dOpts = debtOptions();
  const sOpts = surplusOptions();
  Array.from(closeModal.repayRows.querySelectorAll(".repayRow")).forEach(r=>{
    const sels = r.querySelectorAll("select");
    if (sels.length<2) return;
    const fromSel = sels[0], toSel=sels[1];
    const fromVal=fromSel.value, toVal=toSel.value;

    const rebuild = (sel, options, placeholder, current, labelFn)=>{
      sel.innerHTML="";
      const ph=document.createElement("option");
      ph.value=""; ph.textContent=placeholder;
      sel.appendChild(ph);
      options.forEach(o=>{
        const opt=document.createElement("option");
        opt.value=o; opt.textContent=labelFn(o);
        sel.appendChild(opt);
      });
      sel.value = options.includes(current) ? current : "";
    };

    rebuild(fromSel, sOpts, "返済元（余剰）", fromVal, (c)=>`${c}（余剰 ${yen(closeModal.workingSurplus?.[c]||0)}）`);
    rebuild(toSel, dOpts, "返済先（借金）", toVal, (c)=>`${c}（借金 ${yen(closeModal.workingDebt?.[c]||0)}）`);
  });
}

function confirmClose(){
  const month = closeModal.currentMonth;
  if (!month) return;

  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) { hideCloseModal(); return; }

  const { delta } = computeMonthTotals(month);
  const isDeficit = delta < 0;
  const romanceDeposit = isDeficit ? 5000 : 10000;

  // apply deficits to debt and remember
  const def = deficitsByCat(month);
  const debtAdded = {};
  CATEGORIES.forEach(c=>{
    const amt = def[c] || 0;
    if (amt>0){
      state.debt[c] = (state.debt[c]||0) + amt;
      debtAdded[c] = amt;
    }
  });

  // forced repay
  let forcedRepay = null;
  if (isDeficit){
    const toCat = closeModal.forcedSelect.value;
    state.debt[toCat] = Math.max(0, (state.debt[toCat]||0) - 5000);
    forcedRepay = { toCat, amount: 5000 };
  }

  // optional repayments
  const monthSurplus = computeCategorySurplus(month);
  const rows = parseRepayRows().filter(r=>r.from && r.to && r.amount>0);

  const applied = [];
  rows.forEach(r=>{
    const usedFrom = applied.filter(x=>x.from===r.from).reduce((a,x)=>a+x.amount,0);
    const fromRemain = Math.max(0, (monthSurplus[r.from]||0) - usedFrom);
    const debtAvail = state.debt[r.to] || 0;
    const amt = Math.min(r.amount, fromRemain, debtAvail);
    if (amt>0){
      state.debt[r.to] = Math.max(0, debtAvail - amt);
      applied.push({ from:r.from, to:r.to, amount:amt });
    }
  });

  // romance deposit bookkeeping
  state.romance.balance = (state.romance.balance||0) + romanceDeposit;
  // avoid duplicates if somehow exists
  state.romance.history = (state.romance.history||[]).filter(h=>h.month!==month);
  state.romance.history.push({ month, amount: romanceDeposit });

  m.closed = true;
  m.closeMeta = {
    romanceDeposit,
    debtAdded,
    forcedRepay,
    repayments: applied
  };

  scheduleSave();
  hideCloseModal();
  render();
  alert("締めました");
}

function reopenMonth(){
  const month = getSelectedMonth();
  if (!month) return;
  ensureMonth(month);
  const m = state.months[month];
  if (!m.closed) return;

  if (!confirm("締め解除します。借金・ロマン・返済履歴を差し戻します。OK？")) return;

  const meta = m.closeMeta;
  if (!meta){
    // fallback: just unlock
    m.closed = false;
    scheduleSave();
    render();
    return;
  }

  // reverse repayments (add back to debt)
  (meta.repayments||[]).forEach(r=>{
    state.debt[r.to] = (state.debt[r.to]||0) + (r.amount||0);
  });

  // reverse forced repay
  if (meta.forcedRepay){
    state.debt[meta.forcedRepay.toCat] = (state.debt[meta.forcedRepay.toCat]||0) + (meta.forcedRepay.amount||0);
  }

  // reverse debt added (subtract deficits)
  const debtAdded = meta.debtAdded || {};
  Object.entries(debtAdded).forEach(([cat,amt])=>{
    state.debt[cat] = Math.max(0, (state.debt[cat]||0) - Number(amt||0));
  });

  // reverse romance
  const dep = meta.romanceDeposit || 0;
  state.romance.balance = Math.max(0, (state.romance.balance||0) - dep);
  state.romance.history = (state.romance.history||[]).filter(h=>h.month!==month);

  // unlock
  m.closed = false;
  m.closeMeta = null;

  scheduleSave();
  render();
  alert("締め解除しました");
}

// --- Tx modal (add/edit) ---
const txModal = {
  backdrop:null, closeX:null, cancel:null, saveBtn:null,
  title:null, sub:null, date:null, cat:null, subcat:null, amount:null, memo:null, irregular:null,
  editId: null
};

function openTxModal({ editId=null } = {}){
  const month = getSelectedMonth();
  if (!month) { alert("月を選んでください"); return; }
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) { alert("締め済みの月は編集できません"); return; }

  txModal.editId = editId;
  txModal.title.textContent = editId ? "支出編集" : "支出追加";
  txModal.sub.textContent = month;

  // populate cats
  txModal.cat.innerHTML = "";
  CATEGORIES.forEach(c=>{
    const opt=document.createElement("option");
    opt.value=c; opt.textContent=c;
    txModal.cat.appendChild(opt);
  });

  if (editId){
    const t = (m.tx||[]).find(x=>x.id===editId);
    if (!t) return;
    txModal.date.value = t.date || `${month}-01`;
    txModal.cat.value = t.cat;
    txModal.subcat.value = t.subcat || "";
    txModal.amount.value = t.amount || 0;
    txModal.memo.value = t.memo || "";
    txModal.irregular.checked = !!t.irregular;
  } else {
    txModal.date.value = todayISO();
    // if today is another month, clamp
    if (monthFromDateISO(txModal.date.value) !== month) txModal.date.value = `${month}-01`;
    txModal.cat.value = CATEGORIES[0];
    txModal.subcat.value = "";
    txModal.amount.value = "";
    txModal.memo.value = "";
    txModal.irregular.checked = false;
  }

  txModal.backdrop.classList.remove("hidden");
  txModal.backdrop.setAttribute("aria-hidden","false");
}

function closeTxModal(){
  txModal.backdrop.classList.add("hidden");
  txModal.backdrop.setAttribute("aria-hidden","true");
  txModal.editId = null;
}

function saveTxFromModal(){
  const month = getSelectedMonth();
  ensureMonth(month);
  const m = state.months[month];

  const date = txModal.date.value || `${month}-01`;
  if (monthFromDateISO(date) !== month){
    alert("日付は選択中の月にしてください");
    return;
  }

  const cat = txModal.cat.value;
  const subcat = (txModal.subcat.value||"").trim();
  const amount = clampInt(txModal.amount.value || 0, 0, 10**12);
  const memo = (txModal.memo.value||"").trim();
  const irregular = !!txModal.irregular.checked;

  if (!cat || !CATEGORIES.includes(cat)) { alert("カテゴリを選んでください"); return; }
  if (amount <= 0) { alert("金額を入力してください"); return; }

  if (txModal.editId){
    const t = (m.tx||[]).find(x=>x.id===txModal.editId);
    if (!t) return;
    if (t.kind === "manual"){
      // editing manual tx is allowed but keep it manual
      t.date = date; t.cat=cat; t.subcat=subcat; t.amount=amount; t.memo=memo; t.irregular=false;
    } else if (t.kind === "bulk"){
      // bulk stays bulk
      t.date = date; t.cat=cat; t.subcat=""; t.amount=amount; t.memo=memo || "月次まとめ"; t.irregular=false;
    } else {
      t.date=date; t.cat=cat; t.subcat=subcat; t.amount=amount; t.memo=memo; t.irregular=irregular;
    }
  } else {
    m.tx.push({
      id: uuid(),
      kind: "normal",
      date, cat, subcat, amount, irregular, memo
    });
  }

  scheduleSave();
  closeTxModal();
  render();
}

// --- Budget modal ---
const budgetModal = {
  backdrop:null, closeX:null, cancel:null, saveBtn:null, grid:null
};

const bulkModal = {
  backdrop:null, closeX:null, cancel:null, saveBtn:null, copyPrevBtn:null,
  sub:null, grid:null
};

const settingsModal = {
  backdrop:null, closeX:null, done:null, toggleTxPanel:null
};

const incomeModal = {
  backdrop:null, closeX:null, done:null,
  salary:null, side:null, bonus:null, sumView:null,
  yearIncome:null, yearExpense:null, yearBalance:null
};

const rulesModal = {
  backdrop:null, closeX:null, done:null
};


function openBudgetModal(){
  // show master only
  budgetModal.grid.innerHTML = "";
  CATEGORIES.forEach(cat=>{
    const row = document.createElement("div");
    row.className = "budgetRow";
    row.innerHTML = `
      <div class="name">${cat}</div>
      <input type="number" inputmode="numeric" min="0" step="100" data-cat="${cat}" value="${Number(state.config.budgetMaster[cat]||0)}" />
    `;
    budgetModal.grid.appendChild(row);
  });

  budgetModal.backdrop.classList.remove("hidden");
  budgetModal.backdrop.setAttribute("aria-hidden","false");
}
function closeBudgetModal(){
  budgetModal.backdrop.classList.add("hidden");
  budgetModal.backdrop.setAttribute("aria-hidden","true");
}
function saveBudgetModal(){
  const inputs = Array.from(budgetModal.grid.querySelectorAll("input[data-cat]"));
  inputs.forEach(inp=>{
    const cat = inp.dataset.cat;
    state.config.budgetMaster[cat] = clampInt(inp.value||0, 0, 10**12);
  });
  scheduleSave();
  closeBudgetModal();
  render();
}


function openBulkModal(){
  const month = getSelectedMonth();
  if (!month) { alert("月を選んでください"); return; }
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) { alert("締め済みの月は編集できません"); return; }

  bulkModal.sub.textContent = month;
  bulkModal.grid.innerHTML = "";

  const existing = {};
  (m.tx||[]).forEach(t=>{
    if (t.kind === "bulk") existing[t.cat] = Number(t.amount||0);
  });

  CATEGORIES.forEach(cat=>{
    const row = document.createElement("div");
    row.className = "budgetRow";
    row.innerHTML = `
      <div class="name">${cat}</div>
      <input type="number" inputmode="numeric" min="0" step="100" data-cat="${cat}" value="${existing[cat] ?? ""}" placeholder="0" />
    `;
    bulkModal.grid.appendChild(row);
  });

  bulkModal.backdrop.classList.remove("hidden");
  bulkModal.backdrop.setAttribute("aria-hidden","false");
}

function closeBulkModal(){
  bulkModal.backdrop.classList.add("hidden");
  bulkModal.backdrop.setAttribute("aria-hidden","true");
}

function prevMonthStr(month){
  const [y,m] = month.split("-").map(Number);
  const d = new Date(y, m-1, 1);
  d.setMonth(d.getMonth()-1);
  const yy = d.getFullYear();
  const mm = String(d.getMonth()+1).padStart(2,"0");
  return `${yy}-${mm}`;
}

function copyPrevMonthToBulk(){
  const month = getSelectedMonth();
  ensureMonth(month);
  const prev = prevMonthStr(month);
  ensureMonth(prev);
  const pm = state.months[prev];

  const prevTotals = {};
  CATEGORIES.forEach(c=>prevTotals[c]=0);
  let hasBulk=false;
  (pm.tx||[]).forEach(t=>{
    if (t.kind==="bulk"){ prevTotals[t.cat] = Number(t.amount||0); hasBulk=true; }
  });
  if (!hasBulk){
    const act = actualByCat(prev);
    CATEGORIES.forEach(c=>prevTotals[c]=Number(act[c]||0));
  }

  Array.from(bulkModal.grid.querySelectorAll("input[data-cat]")).forEach(inp=>{
    const cat = inp.dataset.cat;
    const v = prevTotals[cat] || 0;
    inp.value = v ? v : "";
  });
}

function saveBulkModal(){
  const month = getSelectedMonth();
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) return;

  m.tx = (m.tx||[]).filter(t=>t.kind !== "bulk");

  const inputs = Array.from(bulkModal.grid.querySelectorAll("input[data-cat]"));
  inputs.forEach(inp=>{
    const cat = inp.dataset.cat;
    const amount = clampInt(inp.value||0, 0, 10**12);
    if (amount <= 0) return;
    m.tx.push({
      id: "bulk-" + cat,
      kind: "bulk",
      date: `${month}-01`,
      cat,
      subcat: "",
      amount,
      irregular: false,
      memo: "月次まとめ"
    });
  });

  scheduleSave();
  closeBulkModal();
  render();
}

function openSettingsModal(){
  settingsModal.toggleTxPanel.checked = !!state.config.ui.showTxPanel;
  settingsModal.backdrop.classList.remove("hidden");
  settingsModal.backdrop.setAttribute("aria-hidden","false");
}
function closeSettingsModal(){
  settingsModal.backdrop.classList.add("hidden");
  settingsModal.backdrop.setAttribute("aria-hidden","true");
}
function commitSettings(){
  state.config.ui.showTxPanel = !!settingsModal.toggleTxPanel.checked;
  scheduleSave();
  closeSettingsModal();
  render();
}

function syncMonthBudgetFromMaster(){
  const month = getSelectedMonth();
  if (!month) return;
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) return;
  if (!confirm("今月の予算スナップショットを、最新のマスター予算で上書きします。OK？")) return;
  m.budgetSnapshot = deepClone(state.config.budgetMaster);
  scheduleSave();
  render();
}

// --- AI insights (simple) ---
function renderAI(month){
  const box = document.getElementById("aiBox");
  box.innerHTML = "";

  const closed = closedMonthsSorted();
  const last6 = lastNMonths(closed, 6);
  if (last6.length === 0){
    box.innerHTML = `<div class="aiCard"><div class="aiTitle">まだ分析できません</div><div class="aiText">締めた月が増えるほど、改善点の精度が上がります。</div></div>`;
    return;
  }

  // compute overspend frequency in last6
  const overs = {}; CATEGORIES.forEach(c=>overs[c]=0);
  const irr6 = {}; CATEGORIES.forEach(c=>irr6[c]=0);
  last6.forEach(mo=>{
    const bud = budgetForMonth(mo);
    const act = actualByCat(mo);
    const mm = state.months[mo];
    (mm.tx||[]).forEach(t=>{ if (t.irregular) irr6[t.cat]=(irr6[t.cat]||0)+Number(t.amount||0); });
    CATEGORIES.forEach(c=>{
      if ((act[c]||0) > (bud[c]||0)) overs[c] += 1;
    });
  });

  const topOvers = CATEGORIES
    .map(c=>({c, n: overs[c]}))
    .sort((a,b)=>b.n-a.n)
    .filter(x=>x.n>0)
    .slice(0,3);

  const topDebt = CATEGORIES
    .map(c=>({c, amt: state.debt[c]||0}))
    .sort((a,b)=>b.amt-a.amt)
    .filter(x=>x.amt>0)
    .slice(0,3);

  const current = state.months[month];
  const curIrr = (current.tx||[]).filter(t=>t.irregular).reduce((a,t)=>a+Number(t.amount||0),0);
  const { delta } = computeMonthTotals(month);

  // Card 1: This month
  const c1 = document.createElement("div");
  c1.className="aiCard";
  c1.innerHTML = `
    <div class="aiTitle">今月の注意点（ラフ）</div>
    <div class="aiText">
      差分: <b style="color:${delta<0?'#ffb3c2':'#b6f6d0'}">${delta>=0?"+":""}${yen(delta)}</b> /
      イレギュラー: <b>${yen(curIrr)}</b><br>
      目標: 「突発は“借金で吸収”」にしない運用なので、突発の翌月に“どの余剰で返すか”を意識すると制御しやすい。
    </div>
  `;
  box.appendChild(c1);

  // Card 2: recurring overspend
  const c2 = document.createElement("div");
  c2.className="aiCard";
  c2.innerHTML = `
    <div class="aiTitle">過去${last6.length}ヶ月でオーバーしがちなカテゴリ</div>
    <div class="aiText">
      ${topOvers.length? topOvers.map(x=>`・${x.c}: ${x.n}/${last6.length}ヶ月`).join("<br>") : "オーバー傾向はまだ強く出ていません。"}<br>
      対策案: 「予算が現実に合ってない」か「突発が混ざってる」かを切り分ける。突発なら返済ルールで制御、恒常なら予算見直し候補。
    </div>
  `;
  box.appendChild(c2);

  // Card 3: debt focus
  const c3 = document.createElement("div");
  c3.className="aiCard";
  c3.innerHTML = `
    <div class="aiTitle">借金の優先返済候補</div>
    <div class="aiText">
      ${topDebt.length? topDebt.map(x=>`・${x.c}: 借金 ${yen(x.amt)}`).join("<br>") : "借金はありません（かなり健康）。"}<br>
      返済のコツ: 迷ったら「借金が大きい順」or「毎月オーバーしがちな順」から返すと意思決定が速い。
    </div>
  `;
  box.appendChild(c3);

  // Card 4: irregular hotspots
  const topIrr = CATEGORIES
    .map(c=>({c, amt: irr6[c]||0}))
    .sort((a,b)=>b.amt-a.amt)
    .filter(x=>x.amt>0)
    .slice(0,3);

  const c4 = document.createElement("div");
  c4.className="aiCard";
  c4.innerHTML = `
    <div class="aiTitle">突発（イレギュラー）トップ</div>
    <div class="aiText">
      ${topIrr.length? topIrr.map(x=>`・${x.c}: ${yen(x.amt)}（過去${last6.length}ヶ月）`).join("<br>") : "突発はまだ記録が少ないです。"}<br>
      次の一手: 突発が多いカテゴリは、月末の“返済優先枠”を先に決めておくと歯止めが効く。
    </div>
  `;
  box.appendChild(c4);
}

// --- Filters ---
document.querySelectorAll(".seg").forEach(btn=>{
  btn.addEventListener("click", ()=>{
    txFilter = btn.dataset.filter;
    document.querySelectorAll(".seg").forEach(b=>b.classList.toggle("active", b===btn));
    renderTxList(getSelectedMonth());
  });
});
document.getElementById("txSearch").addEventListener("input", ()=>{
  renderTxList(getSelectedMonth());
});

// --- Close modal wiring ---
function wireCloseModal(){
  closeModal.backdrop = document.getElementById("modalBackdrop");
  closeModal.closeX = document.getElementById("modalCloseX");
  closeModal.cancel = document.getElementById("modalCancelBtn");
  closeModal.confirm = document.getElementById("modalConfirmBtn");
  closeModal.closeSummary = document.getElementById("closeSummary");
  closeModal.forcedSection = document.getElementById("forcedRepaySection");
  closeModal.forcedSelect = document.getElementById("forcedToSelect");
  closeModal.repayRows = document.getElementById("repayRows");
  closeModal.addRow = document.getElementById("addRepayRowBtn");
  closeModal.validation = document.getElementById("repayValidation");
  closeModal.sub = document.getElementById("modalSub");
  closeModal.visualPanel = document.getElementById("visualPanel");

  closeModal.closeX.addEventListener("click", hideCloseModal);
  closeModal.cancel.addEventListener("click", hideCloseModal);
  closeModal.backdrop.addEventListener("click", (e)=>{ if (e.target === closeModal.backdrop) hideCloseModal(); });
  closeModal.addRow.addEventListener("click", ()=>{ addRepayRow(); validateRepayments(); });
  closeModal.forcedSelect.addEventListener("change", validateRepayments);
  closeModal.confirm.addEventListener("click", confirmClose);
}

// --- Tx modal wiring ---
function wireTxModal(){
  txModal.backdrop = document.getElementById("txBackdrop");
  txModal.closeX = document.getElementById("txCloseX");
  txModal.cancel = document.getElementById("txCancelBtn");
  txModal.saveBtn = document.getElementById("txSaveBtn");
  txModal.title = document.getElementById("txTitle");
  txModal.sub = document.getElementById("txSub");

  txModal.date = document.getElementById("txDate");
  txModal.cat = document.getElementById("txCat");
  txModal.subcat = document.getElementById("txSubcat");
  txModal.amount = document.getElementById("txAmount");
  txModal.memo = document.getElementById("txMemo");
  txModal.irregular = document.getElementById("txIrregular");

  txModal.closeX.addEventListener("click", closeTxModal);
  txModal.cancel.addEventListener("click", closeTxModal);
  txModal.backdrop.addEventListener("click", (e)=>{ if (e.target === txModal.backdrop) closeTxModal(); });
  txModal.saveBtn.addEventListener("click", saveTxFromModal);
}

// --- Budget modal wiring ---
function wireBudgetModal(){
  budgetModal.backdrop = document.getElementById("budgetBackdrop");
  budgetModal.closeX = document.getElementById("budgetCloseX");
  budgetModal.cancel = document.getElementById("budgetCancelBtn");
  budgetModal.saveBtn = document.getElementById("budgetSaveBtn");
  budgetModal.grid = document.getElementById("budgetGrid");

  budgetModal.closeX.addEventListener("click", closeBudgetModal);
  budgetModal.cancel.addEventListener("click", closeBudgetModal);
  budgetModal.backdrop.addEventListener("click", (e)=>{ if (e.target === budgetModal.backdrop) closeBudgetModal(); });
  budgetModal.saveBtn.addEventListener("click", saveBudgetModal);
}

// --- Events ---
document.getElementById("monthPicker").addEventListener("change", ()=>{
  const month = getSelectedMonth();
  if (!month) return;
  ensureMonth(month);
  scheduleSave();
  render();
});

document.getElementById("bulkInputBtn").addEventListener("click", openBulkModal);

document.getElementById("addTxBtn").addEventListener("click", ()=>openTxModal());

document.getElementById("settingsBtn").addEventListener("click", openSettingsModal);
document.getElementById("editBudgetBtn").addEventListener("click", openBudgetModal);
document.getElementById("syncBudgetBtn").addEventListener("click", syncMonthBudgetFromMaster);

document.getElementById("closeMonthBtn").addEventListener("click", ()=>{
  const month = getSelectedMonth();
  if (!month) { alert("月を選んでください"); return; }
  openCloseModal(month);
});
document.getElementById("reopenMonthBtn").addEventListener("click", reopenMonth);

document.getElementById("resetBtn").addEventListener("click", async ()=>{
  if (!confirm("全データを削除します（localStorage）。本当にOK？")) return;
  state = { config:{ budgetMaster: deepClone(DEFAULT_BUDGET), ui:{showTxPanel:false} }, months:{}, debt:{}, romance:{balance:0,history:[]}, meta:{rev:0} };
  normalizeState();
  await writeRemoteNow("reset");
  render();
  document.querySelector("#budgetTable tbody").innerHTML="";
  document.getElementById("kpiCards").innerHTML="";
  document.getElementById("txList").innerHTML="";
  document.getElementById("aiBox").innerHTML="";
  document.getElementById("debtList").innerHTML="";
  document.getElementById("report").innerHTML="";
  alert("初期化しました");
});

document.getElementById("seedBtn").addEventListener("click", seedSample);

// table input => manual adjustment
document.addEventListener("input", (e)=>{
  const cat = e.target?.dataset?.cat;
  if (!cat) return;
  const month = getSelectedMonth();
  if (!month) return;
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) return;

  const desired = clampInt(e.target.value||0, 0, 10**12);
  upsertManualAdjustment(month, cat, desired);
  scheduleSave();

  // update diff cell and row tone quickly without full rebuild
  const bud = budgetForMonth(month);
  const actual = actualByCat(month)[cat] || 0;
  const diff = (bud[cat]||0) - actual;

  const row = e.target.closest("tr");
  const diffCell = row.querySelector(".diff");
  diffCell.textContent = (diff >= 0 ? "+" : "") + yen(diff);
  diffCell.className = "num diff " + (diff >= 0 ? "good" : "bad");
  if (diff < 0) { row.classList.add("deficit"); row.classList.remove("surplus"); }
  else { row.classList.add("surplus"); row.classList.remove("deficit"); }

  renderKpis(month);
  renderTxList(month);
  renderMonthChips(month);
});

// tx list action buttons
document.getElementById("txList").addEventListener("click", (e)=>{
  const btn = e.target.closest("button[data-act]");
  if (!btn) return;
  const act = btn.dataset.act;
  const id = btn.dataset.id;
  const month = getSelectedMonth();
  ensureMonth(month);
  const m = state.months[month];
  if (m.closed) { alert("締め済みの月は編集できません"); return; }

  if (act === "edit"){
    openTxModal({ editId: id });
  } else if (act === "del"){
    if (!confirm("削除しますか？")) return;
    m.tx = (m.tx||[]).filter(t=>t.id!==id);
    scheduleSave();
    render();
  }
});

// --- Sample data ---
function seedSample(){
  if (!confirm("サンプルデータ（直近3ヶ月）を投入します。既存データは残ります。OK？")) return;

  const today = new Date();
  const months = [];
  for (let i=2;i>=0;i--){
    const d = new Date(today.getTime());
    d.setMonth(d.getMonth()-i);
    const y=d.getFullYear();
    const m=String(d.getMonth()+1).padStart(2,'0');
    months.push(`${y}-${m}`);
  }

  function randFor(key){
    let h=0;
    for (let i=0;i<key.length;i++) h = (h*31 + key.charCodeAt(i)) >>> 0;
    h ^= h << 13; h >>>= 0; h ^= h >> 17; h >>>= 0; h ^= h << 5; h >>>= 0;
    return (h % 1000) / 1000;
  }

  months.forEach((mo, idx)=>{
    ensureMonth(mo);
    const m = state.months[mo];
    if (m.closed) return;

    // make a few transactions per category
    CATEGORIES.forEach(cat=>{
      const b = budgetForMonth(mo)[cat] || 0;
      const r = randFor(mo+"::"+cat);
      let factor = 0.75 + (r*0.5);
      if (idx === 0) factor += 0.12;
      if (idx === 1) factor -= 0.10;
      if (idx === 2) factor += 0.20;

      let target = Math.round(b * factor / 100) * 100;
      if (cat==="住宅") target = b;

      // split into 1-3 tx
      const n = 1 + Math.floor(randFor("n::"+mo+"::"+cat)*3);
      let remain = target;
      for (let i=0;i<n;i++){
        const part = (i===n-1) ? remain : Math.max(0, Math.round((remain*(0.3 + randFor("p::"+i+mo+cat)*0.4))/100)*100);
        remain -= part;
        if (part<=0) continue;
        const day = String(1 + Math.floor(randFor("d::"+i+mo+cat)*27)).padStart(2,'0');
        m.tx.push({
          id: uuid(),
          kind:"normal",
          date: `${mo}-${day}`,
          cat,
          subcat: (randFor("s::"+i+mo+cat)>0.75) ? (cat==="交際費" ? "食事" : (cat==="食費" ? "外食" : "")) : "",
          amount: part,
          irregular: (randFor("irr::"+i+mo+cat)>0.92),
          memo: ""
        });
      }
    });
  });

  // auto close first two months
  const autoClose = (mo)=>{
    const month = mo;
    ensureMonth(month);
    const m = state.months[month];
    if (m.closed) return;
    const { delta } = computeMonthTotals(month);
    const isDeficit = delta < 0;
    const romanceDeposit = isDeficit ? 5000 : 10000;

    const def = deficitsByCat(month);
    const debtAdded = {};
    CATEGORIES.forEach(c=>{
      const amt = def[c]||0;
      if (amt>0){ state.debt[c]=(state.debt[c]||0)+amt; debtAdded[c]=amt; }
    });

    let forcedRepay=null;
    if (isDeficit){
      const biggest = CATEGORIES.map(c=>[c,state.debt[c]||0]).sort((a,b)=>b[1]-a[1])[0]?.[0] || CATEGORIES[0];
      state.debt[biggest]=Math.max(0,(state.debt[biggest]||0)-5000);
      forcedRepay={toCat:biggest,amount:5000};
    }

    state.romance.balance=(state.romance.balance||0)+romanceDeposit;
    state.romance.history = (state.romance.history||[]).filter(h=>h.month!==month);
    state.romance.history.push({month,amount:romanceDeposit});

    m.closed=true;
    m.closeMeta={romanceDeposit,debtAdded,forcedRepay,repayments:[]};
  };
  autoClose(months[0]);
  autoClose(months[1]);

  scheduleSave();
  document.getElementById("monthPicker").value = months[2];
  render();
  alert("サンプル投入しました（過去2ヶ月は締め済み）。");
}


function wireBulkModal(){
  bulkModal.backdrop = document.getElementById("bulkBackdrop");
  bulkModal.closeX = document.getElementById("bulkCloseX");
  bulkModal.cancel = document.getElementById("bulkCancelBtn");
  bulkModal.saveBtn = document.getElementById("bulkSaveBtn");
  bulkModal.copyPrevBtn = document.getElementById("bulkCopyPrevBtn");
  bulkModal.sub = document.getElementById("bulkSub");
  bulkModal.grid = document.getElementById("bulkGrid");

  bulkModal.closeX.addEventListener("click", closeBulkModal);
  bulkModal.cancel.addEventListener("click", closeBulkModal);
  bulkModal.backdrop.addEventListener("click", (e)=>{ if (e.target === bulkModal.backdrop) closeBulkModal(); });
  bulkModal.saveBtn.addEventListener("click", saveBulkModal);
  bulkModal.copyPrevBtn.addEventListener("click", copyPrevMonthToBulk);
}

function wireSettingsModal(){
  settingsModal.backdrop = document.getElementById("settingsBackdrop");
  settingsModal.closeX = document.getElementById("settingsCloseX");
  settingsModal.done = document.getElementById("settingsDoneBtn");
  settingsModal.toggleTxPanel = document.getElementById("toggleTxPanel");

  settingsModal.closeX.addEventListener("click", closeSettingsModal);
  settingsModal.done.addEventListener("click", commitSettings);
  settingsModal.backdrop.addEventListener("click", (e)=>{ if (e.target === settingsModal.backdrop) closeSettingsModal(); });
}

function currentMonthKey(){
  const mp = document.getElementById("monthPicker");
  return mp?.value || ymNow();
}

function getMonthIncome(month){
  const m = state.months[month] || (state.months[month] = { closed:false, budgetSnapshot:null, tx:[] });
  if (!m.income) m.income = { salary:0, side:0, bonus:0 };
  return m.income;
}

function incomeTotalOf(month){
  const inc = getMonthIncome(month);
  return Number(inc.salary||0) + Number(inc.side||0) + Number(inc.bonus||0);
}

// Romance outflow (支出扱い) for annual / balance display.
// Closed month: use closeMeta (deposit + forced repay) if available. Otherwise use plan from current delta.
function romanceOutflowOf(month){
  const m = state.months[month] || {};
  if (m.closed && m.closeMeta){
    const dep = Number(m.closeMeta.romanceDeposit||0);
    const fr = Number(m.closeMeta.forcedRepay?.amount||0);
    return dep + fr;
  }
  const { delta } = computeMonthTotals(month);
  if (delta < 0) return 10000; // 5k deposit + 5k repay
  // black / break-even: try 15k if still non-negative after romance
  return (delta - 15000 >= 0) ? 15000 : 10000;
}

function annualTotals(year){
  const prefix = `${year}-`;
  let yIncome = 0;
  let yExpense = 0;

  Object.keys(state.months||{}).forEach(k=>{
    if (!k.startsWith(prefix)) return;
    const { totalActual } = computeMonthTotals(k);
    yIncome += incomeTotalOf(k);
    yExpense += Number(totalActual||0) + romanceOutflowOf(k);
  });

  return { yIncome, yExpense, yBalance: yIncome - yExpense };
}

function openIncomeModal(){
  const month = currentMonthKey();
  const inc = getMonthIncome(month);

  incomeModal.salary.value = inc.salary ?? 0;
  incomeModal.side.value = inc.side ?? 0;
  incomeModal.bonus.value = inc.bonus ?? 0;

  const update = ()=>{
    const s = Number(incomeModal.salary.value||0);
    const si = Number(incomeModal.side.value||0);
    const b = Number(incomeModal.bonus.value||0);
    const sum = s+si+b;
    incomeModal.sumView.textContent = yen(sum);

    const y = Number(month.split("-")[0]);
    const { yIncome, yExpense, yBalance } = annualTotals(y);
    incomeModal.yearIncome.textContent = yen(yIncome);
    incomeModal.yearExpense.textContent = yen(yExpense);
    incomeModal.yearBalance.textContent = `${yBalance>=0?"+":""}${yen(yBalance)}`;
  };

  incomeModal.salary.oninput = update;
  incomeModal.side.oninput = update;
  incomeModal.bonus.oninput = update;

  update();
  incomeModal.backdrop.classList.remove("hidden");
  incomeModal.backdrop.setAttribute("aria-hidden","false");
}

function closeIncomeModal(){
  incomeModal.backdrop.classList.add("hidden");
  incomeModal.backdrop.setAttribute("aria-hidden","true");
}

async function saveIncomeModal(){
  const month = currentMonthKey();
  const inc = getMonthIncome(month);

  inc.salary = Number(incomeModal.salary.value||0);
  inc.side = Number(incomeModal.side.value||0);
  inc.bonus = Number(incomeModal.bonus.value||0);

  // sync + rerender
  await saveState();
  closeIncomeModal();
  render();
}

function openRulesModal(){
  rulesModal.backdrop.classList.remove("hidden");
  rulesModal.backdrop.setAttribute("aria-hidden","false");
}
function closeRulesModal(){
  rulesModal.backdrop.classList.add("hidden");
  rulesModal.backdrop.setAttribute("aria-hidden","true");
}

function wireIncomeModal(){
  incomeModal.backdrop = document.getElementById("incomeBackdrop");
  incomeModal.closeX = document.getElementById("incomeCloseX");
  incomeModal.done = document.getElementById("incomeDoneBtn");
  incomeModal.salary = document.getElementById("incomeSalary");
  incomeModal.side = document.getElementById("incomeSide");
  incomeModal.bonus = document.getElementById("incomeBonus");
  incomeModal.sumView = document.getElementById("incomeSumView");
  incomeModal.yearIncome = document.getElementById("yearIncomeView");
  incomeModal.yearExpense = document.getElementById("yearExpenseView");
  incomeModal.yearBalance = document.getElementById("yearBalanceView");

  const incomeBtn = document.getElementById("incomeBtn");
  incomeBtn?.addEventListener("click", openIncomeModal);

  incomeModal.closeX.addEventListener("click", closeIncomeModal);
  incomeModal.backdrop.addEventListener("click",(e)=>{ if (e.target === incomeModal.backdrop) closeIncomeModal(); });
  incomeModal.done.addEventListener("click", saveIncomeModal);
}

function wireRulesModal(){
  rulesModal.backdrop = document.getElementById("rulesBackdrop");
  rulesModal.closeX = document.getElementById("rulesCloseX");
  rulesModal.done = document.getElementById("rulesDoneBtn");
  const rulesBtn = document.getElementById("rulesBtn");
  rulesBtn?.addEventListener("click", openRulesModal);

  rulesModal.closeX.addEventListener("click", closeRulesModal);
  rulesModal.done.addEventListener("click", closeRulesModal);
  rulesModal.backdrop.addEventListener("click",(e)=>{ if (e.target === rulesModal.backdrop) closeRulesModal(); });
}



// --- init wiring + boot ---
wireCloseModal();
wireTxModal();
wireBudgetModal();
wireBulkModal();
wireSettingsModal();
wireIncomeModal();
wireRulesModal();

// initialize month picker to current month if empty

// --- Auth UI ---
const authGate = document.getElementById("authGate");
const loginBtn = document.getElementById("loginBtn");
const gateLoginBtn = document.getElementById("gateLoginBtn");
const logoutBtn = document.getElementById("logoutBtn");
const authUser = document.getElementById("authUser");

function showGate(show){
  if (!authGate) return;
  authGate.classList.toggle("show", !!show);
}

async function doLogin(){
  console.log("LOGIN BUTTON CLICKED");   // ←この行を追加

  try{
    const provider = new GoogleAuthProvider();

    const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);

    if (isIOS){
      await signInWithRedirect(auth, provider);
      return;
    }

    await signInWithPopup(auth, provider);

  }catch(e){
    console.error(e);
  }
}
async function doLogout(){
  try{
    await signOut(auth);
  } catch(e){
    console.error(e);
  }
}

loginBtn?.addEventListener("click", doLogin);
gateLoginBtn?.addEventListener("click", doLogin);
logoutBtn?.addEventListener("click", doLogout);

// Complete redirect sign-in (iOS) - do it early
(async ()=>{
  try{
    setSyncChip("同期: ログイン確認中…");

    const result = await getRedirectResult(auth);

    // result がある = リダイレクトログイン直後の復帰
    if (result?.user){
      console.log("Redirect login success:", result.user.email);
      // ここでは同期チップを "-" に戻さない（onAuthStateChangedで確定させる）
      // ※ iOS Safariは auth state の反映がワンテンポ遅れることがあるため
      return;
    }

  } catch(e){
    // "redirect が無い" だけなら何も起きないので握りつぶしてOK
    // ただし本当のエラーも混ざるのでコードだけは出しておく
    console.warn("getRedirectResult:", e?.code || e);

  } finally {
    // 既にログイン済みなら onAuthStateChanged 側で同期表示が更新されるはず
    // 未ログイン時だけ "-" に戻す（ログイン復帰直後に戻すとループに見えることがある）
    if (!auth.currentUser){
      setSyncChip("同期: -");
    }
  }
})();

onAuthStateChanged(auth, async (user)=>{
  if (!user){
    uid = null;
    stateDocRef = null;
    if (authUser) authUser.textContent = "未ログイン";
    loginBtn?.classList.remove("hidden");
    logoutBtn?.classList.add("hidden");
    setSyncChip("同期: -");
    showGate(true);
    return;
  }

  uid = user.uid;
  if (authUser) authUser.textContent = user.email || user.displayName || "ログイン中";
  loginBtn?.classList.add("hidden");
  logoutBtn?.classList.remove("hidden");
  showGate(false);

  // doc: users/{uid}/apps/budget
  stateDocRef = doc(db, "users", uid, "apps", "budget");
  await loadFromRemote();
});


(function initMonth(){
  const mp = document.getElementById("monthPicker");
  if (!mp.value){
    const d = new Date();
    const y=d.getFullYear();
    const m=String(d.getMonth()+1).padStart(2,'0');
    mp.value = `${y}-${m}`;
  }
  ensureMonth(mp.value);
  scheduleSave();
  render();
})();

// ===== 最後に必ずイベント接続（DOM構築後に実行）=====
window.addEventListener("DOMContentLoaded", () => {
  const loginBtn = document.getElementById("loginBtn");
  const gateLoginBtn = document.getElementById("gateLoginBtn");

  console.log("wire login buttons:", !!loginBtn, !!gateLoginBtn);

  if (loginBtn) {
    loginBtn.addEventListener("click", () => {
      console.log("LOGIN BUTTON CLICKED");
      doLogin();
    });
  }
  if (gateLoginBtn) {
    gateLoginBtn.addEventListener("click", () => {
      console.log("GATE LOGIN CLICKED");
      doLogin();
    });
  }
});