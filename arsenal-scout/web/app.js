const state = {report:null, workflow:null, reportCache:new Map()};
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[char]));
const formatMoney = value => {
  const number = Number(value);
  const amount = Math.abs(number).toFixed(Math.abs(number) % 1 ? 1 : 0);
  return `${number < 0 ? '−' : ''}€${amount}M`;
};

async function getJSON(url) {
  const response = await fetch(url, {cache:'no-store'});
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

function financeCard(finance) {
  return `<section class="finance-card">
    <div class="finance-card-head"><strong>今回の評価に使ったクラブ財政</strong><span>架空のデモ値</span></div>
    <div class="finance-metrics">
      <div><span>使用可能な補強予算</span><strong>${formatMoney(finance.usable_transfer_budget_m)}</strong></div>
      <div><span>年間賃金の余力</span><strong>${formatMoney(finance.annual_wage_headroom_m)}</strong></div>
      <div><span>1選手あたりの移籍金目安</span><strong>${formatMoney(finance.max_single_fee_guideline_m)}</strong></div>
    </div>
    <p class="formula">算出：${formatMoney(finance.transfer_budget_m)} − 確定済み支出 ${formatMoney(finance.committed_transfer_spend_m)} ＋ 売却見込み ${formatMoney(finance.expected_sales_m)} − 予備費 ${formatMoney(finance.protected_cash_reserve_m)} ＝ ${formatMoney(finance.usable_transfer_budget_m)}</p>
  </section>`;
}

function candidateCard(candidate) {
  const finance = candidate.financial_assessment;
  return `<article class="candidate-card">
    <div class="candidate-top">
      <div class="rank-badge">${candidate.rank}</div>
      <div class="candidate-name"><strong>${esc(candidate.name)}</strong><span>${esc(candidate.club)} · ${candidate.age}歳 · ${esc(candidate.roles.join('／'))}</span></div>
      <div class="total-score">${candidate.score.toFixed(1)}<small>総合評価</small></div>
    </div>
    <p class="candidate-reason">${esc(candidate.why)}</p>
    <div class="deal-grid">
      <div><span>移籍金</span><b>${formatMoney(candidate.estimated_fee_m)}</b></div>
      <div><span>推定年俸</span><b>${formatMoney(finance.annual_wage_m)}</b></div>
      <div><span>初年度の負担</span><b>${formatMoney(finance.first_year_cost_m)}</b></div>
      <div><span>獲得後の予算</span><b>${formatMoney(finance.budget_after_deal_m)}</b></div>
    </div>
    <div class="score-row"><span>戦術適合度</span><div class="score-track"><i style="width:${candidate.tactical_fit}%"></i></div><b>${candidate.tactical_fit.toFixed(1)}</b></div>
    <div class="score-row finance"><span>財政適合度</span><div class="score-track"><i style="width:${candidate.financial_fit}%"></i></div><b>${candidate.financial_fit.toFixed(1)}</b></div>
    <div class="candidate-actions"><button class="detail-button" type="button" data-player="${esc(candidate.player_id)}">評価の内訳を見る</button></div>
  </article>`;
}

function welcomeContent() {
  return `<h1>どのような補強候補を探しますか？</h1>
    <p class="muted">試合データからチームの弱点を整理し、クラブの移籍予算と賃金余力も考慮して候補を比較します。</p>
    <div class="suggestion-grid">
      <button class="suggestion" type="button" data-command="財政負担を抑えながら、ローブロックを崩せる23歳以下の右WGをランキング"><strong>右ウイングを探す</strong><span>財政余力とローブロック攻略を重視</span></button>
      <button class="suggestion" type="button" data-command="25歳以下で、稼働率の高い即戦力の6番をランキング"><strong>守備的MFを探す</strong><span>即戦力と稼働率を重視</span></button>
      <button class="suggestion" type="button" data-command="ライスと共存できる8番を、費用対効果も含めてランキング"><strong>ライスと共存できる8番</strong><span>創造性と守備への切り替えを両立</span></button>
      <button class="suggestion" type="button" data-action="finance"><strong>クラブ財政を確認する</strong><span>予算の算出方法と評価への反映を表示</span></button>
    </div>
    <div class="demo-warning"><strong>ご注意：</strong> この画面の試合、選手、移籍金、年俸、クラブ財政はすべて架空です。</div>`;
}

function rankingContent(report) {
  const conditions = report.instruction.labels.length ? report.instruction.labels : ['追加条件なし：総合評価'];
  if (!report.candidates.length) {
    return `<p>指定された条件を満たす候補は見つかりませんでした。</p><div class="condition-list">${conditions.map(label => `<span>${esc(label)}</span>`).join('')}</div><div class="empty-result"><strong>条件を少し緩めてください。</strong><p>年齢、移籍金、役割、稼働率のいずれかを変更すると、候補が見つかる可能性があります。</p></div>`;
  }
  return `<p>${esc(report.summary.summary)}</p>
    <div class="condition-list">${conditions.map(label => `<span>${esc(label)}</span>`).join('')}</div>
    ${financeCard(report.club_finance)}
    <h2>候補ランキング</h2>
    <div class="candidate-stack">${report.candidates.map(candidateCard).join('')}</div>
    <p class="small">総合評価は、戦術適合度、稼働率、年齢、財政適合度を合成しています。費用対効果を指定した場合は、財政適合度の比重を引き上げます。</p>`;
}

function financeContent(report) {
  return `<p>クラブの財政状況は、候補の<strong>財政適合度</strong>としてランキングへ反映しています。</p>${financeCard(report.club_finance)}
    <div class="info-list">
      <div class="info-row"><strong>移籍金の負担</strong><p>候補の移籍金が、使用可能な補強予算の何％を占めるかを評価します。</p></div>
      <div class="info-row"><strong>賃金の負担</strong><p>推定年俸が、年間賃金の余力をどの程度使うかを評価します。</p></div>
      <div class="info-row"><strong>初年度の負担</strong><p>移籍金を契約年数で割った額に推定年俸を加え、単年の負担を簡易計算します。</p></div>
      <div class="info-row"><strong>獲得後に残る予算</strong><p>一人を獲得したあと、追加補強に使える予算がどれだけ残るかを表示します。</p></div>
    </div><p class="small">これは財務会計やPSR判定を再現するものではなく、候補比較のための簡易モデルです。</p>`;
}

function weaknessContent(report) {
  return `<p>デモ試合の指標を同じ条件の基準値と比較すると、次の課題が優先されます。</p><div class="weakness-list">${report.weaknesses.map(item => `<article class="weakness-item"><div class="weakness-title"><b>${esc(item.label)}</b><b>${item.severity.toFixed(1)}</b></div><p>${esc(item.description)}</p></article>`).join('')}</div><p class="small">深刻度は、基準値との差、試合数、指標の重要度をまとめた0〜100のデモスコアです。</p>`;
}

function evidenceContent(report) {
  const tactical = report.graphrag.paths.length;
  const financial = report.graphrag.financial_paths.length;
  return `<p>推薦は文章だけで作らず、ナレッジグラフ上の経路へ接続しています。</p>
    <div class="path-card"><code>試合指標 → チームの弱点 → 必要な役割 → 候補選手</code><br>${tactical}本の経路を確認</div>
    <div class="path-card"><code>クラブ → 財政スナップショット → 獲得費用 → 候補選手</code><br>${financial}本の経路を確認</div>
    <p>検索では、語句一致と意味の近さを組み合わせ、試合、弱点、選手、クラブ財政を横断します。</p>`;
}

const workflowLabels = {validate:'入力を検査',parse_instruction:'日本語を解析',diagnose:'弱点を診断',finance:'財政余力を計算',retrieve:'根拠を検索',rank:'候補を採点',graph_expand:'関係を接続',explain:'回答を作成'};
function systemContent() {
  const nodes = state.workflow?.nodes || [];
  return `<p>分析は、同じ入力なら同じ結果になる処理の流れとして実装しています。</p><div class="workflow-line">${nodes.map((node,index) => `${index ? '<i>→</i>' : ''}<span>${esc(workflowLabels[node.id] || node.id)}</span>`).join('')}</div>
    <div class="info-list"><div class="info-row"><strong>自然言語の解析</strong><p>外部AIへ送信せず、対応語彙とルールを使って条件を読み取ります。</p></div><div class="info-row"><strong>ハイブリッド検索</strong><p>BM25の語句一致と384次元のローカルベクトルを組み合わせます。</p></div><div class="info-row"><strong>GraphRAG</strong><p>弱点、役割、選手、獲得費用の関係をたどり、推薦理由を確認できるようにします。</p></div><div class="info-row"><strong>監視</strong><p>各処理の時間、検索件数、候補数、エラーを記録します。</p></div></div>`;
}

function assistantMessage(content, id='') { return `<article class="message assistant"${id ? ` id="${id}"` : ''}><div class="assistant-avatar">A</div><div class="message-body">${content}</div></article>`; }
function userMessage(text) { return `<article class="message user"><div class="user-bubble">${esc(text)}</div></article>`; }
function inner() { return $('conversation').querySelector('.conversation-inner'); }
function scrollToBottom() { requestAnimationFrame(() => { $('conversation').scrollTop = $('conversation').scrollHeight; }); }
function appendAssistant(content,id='') { inner().insertAdjacentHTML('beforeend',assistantMessage(content,id)); scrollToBottom(); }
function appendUser(text) { inner().insertAdjacentHTML('beforeend',userMessage(text)); scrollToBottom(); }
function closeSidebar() { document.body.classList.remove('sidebar-open'); }

function resetConversation() {
  $('conversation').innerHTML = '<div class="conversation-inner"></div>';
  appendAssistant(welcomeContent());
}

async function fetchReport(query='') {
  const report = await getJSON(`/api/report?q=${encodeURIComponent(query)}`);
  state.report = report;
  state.reportCache.set(query,report);
  return report;
}

async function runPrompt(text) {
  const query = text.trim();
  if (!query) return;
  closeSidebar(); appendUser(query);
  const typingId = `typing-${Date.now()}`;
  appendAssistant('<div class="typing"><i></i><i></i><i></i></div>',typingId);
  $('sendButton').disabled = true;
  try {
    const report = await fetchReport(query);
    $(typingId).outerHTML = assistantMessage(rankingContent(report));
  } catch (error) {
    $(typingId).outerHTML = assistantMessage(`<p>分析を完了できませんでした。少し待ってから、もう一度お試しください。</p><p class="small">${esc(error.message)}</p>`);
  } finally { $('sendButton').disabled = false; scrollToBottom(); }
}

async function runAction(action) {
  const labels = {scout:'補強候補を探したい',finance:'クラブの財政状況を説明して',weakness:'試合データから優先課題を教えて',evidence:'候補を推薦した根拠を確認したい',system:'この分析の仕組みを説明して'};
  if (!labels[action]) return;
  closeSidebar(); appendUser(labels[action]);
  if (action==='scout') { appendAssistant(welcomeContent()); return; }
  try {
    const report = state.report || await fetchReport('');
    const content = action==='finance' ? financeContent(report) : action==='weakness' ? weaknessContent(report) : action==='evidence' ? evidenceContent(report) : systemContent();
    appendAssistant(content);
  } catch (error) { appendAssistant(`<p>情報を取得できませんでした。</p><p class="small">${esc(error.message)}</p>`); }
  document.querySelectorAll('.side-item').forEach(button => button.classList.toggle('active',button.dataset.action===action));
}

function showCandidate(candidate) {
  if (!candidate) return;
  const finance = candidate.financial_assessment;
  $('candidateDetail').innerHTML = `<div class="detail-heading"><p class="small">ランキング ${candidate.rank}位</p><h2>${esc(candidate.name)}</h2><p>${esc(candidate.club)} · ${esc(candidate.league)} · ${candidate.age}歳</p></div><div class="detail-score">${candidate.score.toFixed(1)} <span>総合評価</span></div><div class="detail-section"><h3>推薦理由</h3><p>${esc(candidate.why)}</p></div><div class="detail-section"><h3>財政面の評価：${esc(finance.status)}</h3><p>${esc(finance.rationale)} 移籍金は使用可能な予算の${finance.budget_share_pct.toFixed(0)}％、推定年俸は賃金余力の${finance.wage_headroom_share_pct.toFixed(0)}％です。</p></div><div class="detail-section"><h3>確認が必要な点</h3><p>${esc(candidate.risk)}</p></div><div class="detail-section"><h3>課題別の適合度</h3><div class="weakness-list">${candidate.weakness_fits.map(item => `<div class="weakness-item"><div class="weakness-title"><b>${esc(item.weakness_label)} → ${esc(item.role_label)}</b><b>${item.fit.toFixed(1)}</b></div></div>`).join('')}</div></div>`;
  $('candidateDialog').showModal();
}

$('chatForm').addEventListener('submit',event=>{event.preventDefault();const input=$('chatInput');runPrompt(input.value);input.value='';input.style.height='auto';});
$('chatInput').addEventListener('input',event=>{event.target.style.height='auto';event.target.style.height=`${Math.min(event.target.scrollHeight,160)}px`;$('sendButton').disabled=!event.target.value.trim();});
$('chatInput').addEventListener('keydown',event=>{if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();$('chatForm').requestSubmit();}});
$('conversation').addEventListener('click',event=>{const command=event.target.closest('[data-command]');const action=event.target.closest('[data-action]');const detail=event.target.closest('[data-player]');if(command)runPrompt(command.dataset.command);if(action)runAction(action.dataset.action);if(detail){const response=detail.closest('.message');const query=response?.previousElementSibling?.querySelector('.user-bubble')?.textContent||'';showCandidate(state.reportCache.get(query)?.candidates.find(item=>item.player_id===detail.dataset.player));}});
document.querySelectorAll('.history [data-command]').forEach(button=>button.addEventListener('click',()=>runPrompt(button.dataset.command)));
document.querySelectorAll('.side-item').forEach(button=>button.addEventListener('click',()=>runAction(button.dataset.action)));
$('newChat').addEventListener('click',()=>{closeSidebar();resetConversation();$('chatInput').focus();});
$('menuButton').addEventListener('click',()=>document.body.classList.add('sidebar-open'));
$('sidebarClose').addEventListener('click',closeSidebar);$('sidebarOverlay').addEventListener('click',closeSidebar);
$('candidateDialog').querySelector('.dialog-close').addEventListener('click',()=>$('candidateDialog').close());

async function init() {
  resetConversation(); $('sendButton').disabled=true;
  try {
    const health = await getJSON('/api/health');
    state.workflow = await getJSON('/api/workflow');
    $('healthText').textContent = `財政反映中 · ${health.documents}件の根拠`;
    const example='クラブの財政余力を守りながら、ローブロック攻略を最優先できる23歳以下の右WGをランキング';
    appendUser(example); appendAssistant('<div class="typing"><i></i><i></i><i></i></div>','initialTyping');
    const report = await fetchReport(example);
    $('initialTyping').outerHTML = assistantMessage(rankingContent(report)); scrollToBottom();
  } catch (error) { $('healthText').textContent='分析エンジンに接続できません'; appendAssistant(`<p>初期分析を取得できませんでした。</p><p class="small">${esc(error.message)}</p>`); }
}

init();
