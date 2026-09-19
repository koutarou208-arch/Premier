const state={report:null,workflow:null,telemetry:null};
const $=id=>document.getElementById(id);
const esc=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[char]));
const pct=value=>`${Number(value).toFixed(1)}%`;

async function getJSON(url){const response=await fetch(url,{cache:'no-store'});if(!response.ok)throw new Error(`${response.status} ${response.statusText}`);return response.json()}

function weaknessCard(item,index){const lead=item.signals[0];return `<article class="weakness-card">
  <span class="weakness-number">SIGNAL / ${String(index+1).padStart(2,'0')} · ${esc(item.status.toUpperCase())}</span>
  <h3>${esc(item.label)}</h3><p>${esc(item.description)}</p>
  <div class="severity-row"><b>${item.severity.toFixed(1)}</b><span>SEVERITY / CONF ${(item.confidence*100).toFixed(0)}%</span></div>
  <div class="severity-track"><i style="width:${item.severity}%"></i></div>
  <div class="signal-mini"><span>${esc(lead.metric_label)}</span><b>Z ${lead.deficiency_z>0?'+':''}${lead.deficiency_z}</b></div>
</article>`}

function candidateRow(item){return `<article class="candidate-row" data-player="${esc(item.player_id)}" tabindex="0">
  <b class="candidate-rank">${String(item.rank).padStart(2,'0')}</b>
  <div class="candidate-name"><b>${esc(item.name)}</b><span>${esc(item.club)} · ${esc(item.league)}</span></div>
  <div class="candidate-cell"><span>AGE / FEE</span><b>${item.age} / €${item.estimated_fee_m}M</b></div>
  <div class="candidate-cell"><span>TACTICAL FIT</span><b>${pct(item.tactical_fit)}</b></div>
  <div class="role-chips">${item.roles.map(role=>`<i>${esc(role)}</i>`).join('')}</div>
  <div class="candidate-score">${item.score.toFixed(1)}</div>
</article>`}

function showCandidate(item){$('candidateDetail').innerHTML=`<p class="eyebrow">RANK ${item.rank} / EXPLAINABLE FIT</p><h2 class="detail-title">${esc(item.name)}</h2>
  <p class="detail-meta">${esc(item.club)} · ${esc(item.league)} · AGE ${item.age} · DEMO FEE €${item.estimated_fee_m}M</p>
  <div class="detail-score">${item.score.toFixed(1)}</div><p>${esc(item.why)}</p><p class="detail-meta">RISK — ${esc(item.risk)}</p>
  <div class="fit-list">${item.weakness_fits.map(fit=>`<div class="fit-row"><div class="fit-row-head"><b>${esc(fit.weakness_label)} → ${esc(fit.role_label)}</b><b>${fit.fit.toFixed(1)}</b></div><p>${fit.top_metrics.map(metric=>`${esc(metric.metric)} ${metric.value}`).join(' · ')}</p></div>`).join('')}</div>`;
  $('candidateDialog').showModal();
}

function renderMission(){const report=state.report;$('priorityCount').textContent=report.priority_count;$('candidateCount').textContent=report.candidates.length;$('pathCount').textContent=report.graphrag.paths.length;$('traceId').textContent=report.meta.trace_id.slice(0,8).toUpperCase();$('disclaimer').innerHTML=`<b>DEMO DATA</b> — ${esc(report.meta.disclaimer)} 候補・価格も架空データです。実データ接続後に意思決定へ使用してください。`;$('weaknessGrid').innerHTML=report.weaknesses.map(weaknessCard).join('');$('candidateList').innerHTML=report.candidates.map(candidateRow).join('');document.querySelectorAll('.candidate-row').forEach(row=>{const open=()=>showCandidate(report.candidates.find(item=>item.player_id===row.dataset.player));row.onclick=open;row.onkeydown=event=>{if(event.key==='Enter')open()}})}

function renderSearch(results){$('searchResults').innerHTML=results.map((row,index)=>`<article class="result-card">
  <div class="result-kind">${String(index+1).padStart(2,'0')} / ${esc(row.kind.toUpperCase())}</div><div><h3>${esc(row.title)}</h3><p>${esc(row.text)}</p></div>
  <div class="score-bars"><span>LEXICAL</span><i><span style="width:${row.lexical_score*100}%;background:var(--gold)"></span></i><b>${(row.lexical_score*100).toFixed(0)}</b><span>VECTOR</span><i><span style="width:${row.vector_score*100}%;background:var(--blue)"></span></i><b>${(row.vector_score*100).toFixed(0)}</b><span>FUSED</span><i><span style="width:${row.score*100}%;background:var(--red2)"></span></i><b>${(row.score*100).toFixed(0)}</b></div>
</article>`).join('')||'<p class="detail-meta">No evidence found.</p>'}

function graphLayout(report){const chosenWeak=report.weaknesses.slice(0,4);const chosenPlayers=report.candidates.slice(0,5);const roles=[...new Set(chosenWeak.flatMap(item=>item.target_roles))].slice(0,6);const nodes=[{id:'team:arsenal',kind:'team',label:'ARSENAL',x:105,y:310}];chosenWeak.forEach((item,i)=>nodes.push({id:`weakness:${item.id}`,kind:'Weakness',label:item.label,x:330,y:110+i*135}));roles.forEach((role,i)=>nodes.push({id:`role:${role}`,kind:'TacticalRole',label:role.replaceAll('_',' '),x:620,y:75+i*94}));chosenPlayers.forEach((item,i)=>nodes.push({id:`player:${item.player_id}`,kind:'Player',label:item.name.replace('Demo · ',''),x:930,y:105+i*105}));const ids=new Set(nodes.map(n=>n.id));const edges=report.graphrag.edges.filter(edge=>ids.has(edge.source)&&ids.has(edge.target)&&['HAS_WEAKNESS','NEEDS_ROLE','CAN_PLAY','ADDRESSES'].includes(edge.relation));return{nodes,edges}}

function renderGraph(){const {nodes,edges}=graphLayout(state.report),byId=Object.fromEntries(nodes.map(node=>[node.id,node]));$('knowledgeGraph').innerHTML=`<defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#385044"/></marker></defs>${edges.map(edge=>{const a=byId[edge.source],b=byId[edge.target];return `<line class="graph-edge" x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}" marker-end="url(#arrow)"><title>${esc(edge.relation)}</title></line>`}).join('')}${nodes.map(node=>`<g class="graph-node ${node.kind}" transform="translate(${node.x},${node.y})"><circle r="${node.kind==='team'?44:32}"></circle><text text-anchor="middle" y="${node.kind==='team'?4:2}">${esc(node.label.length>18?node.label.slice(0,17)+'…':node.label)}</text></g>`).join('')}`;$('pathList').innerHTML=state.report.graphrag.paths.slice(0,9).map(path=>`<div class="path-item"><b>${esc(path.from.replace('weakness:',''))}</b><br>${path.relations.map(esc).join(' → ')}<br><b>${esc(path.to.replace('player:',''))}</b></div>`).join('')}

function renderSystem(){const workflow=state.workflow.nodes;$('workflowGraph').innerHTML=workflow.map((node,index)=>`${index?'<span class="workflow-arrow">→</span>':''}<div class="workflow-node"><b>${esc(node.id.toUpperCase())}</b><span>${esc(node.description)}</span></div>`).join('');const t=state.telemetry;$('telemetry').innerHTML=`<div><span>ANALYSES</span><b>${t.counters.analyses_total||0}</b></div><div><span>SPANS</span><b>${t.counters.spans_total||0}</b></div><div><span>P50 LATENCY</span><b>${t.latency_ms.p50.toFixed(2)}ms</b></div><div><span>P95 LATENCY</span><b>${t.latency_ms.p95.toFixed(2)}ms</b></div><div><span>TOP FIT</span><b>${t.gauges.last_top_candidate_score||0}</b></div><div><span>ERRORS</span><b>${t.counters.errors_total||0}</b></div>`;$('spanRows').innerHTML=t.recent_spans.slice(0,14).map(span=>`<tr><td>${esc(span.name)}</td><td class="${span.status==='ok'?'ok-status':''}">${esc(span.status)}</td><td>${span.duration_ms.toFixed(3)}ms</td><td>${esc(span.trace_id.slice(0,10))}</td></tr>`).join('')}

async function loadReport(){const budget=$('budgetInput').value;$('budgetValue').textContent=budget;state.report=await getJSON(`/api/report?budget_m=${budget}`);renderMission();renderGraph();state.telemetry=await getJSON('/api/observability');renderSystem()}

async function init(){try{const health=await getJSON('/api/health');$('healthText').textContent=`ENGINE ONLINE / ${health.documents} DOCS`;state.workflow=await getJSON('/api/workflow');await loadReport();const initial=await getJSON(`/api/search?q=${encodeURIComponent($('searchInput').value)}`);renderSearch(initial.results)}catch(error){$('healthText').textContent='ENGINE ERROR';$('disclaimer').textContent=`起動エラー: ${error.message}`;console.error(error)}}

document.querySelectorAll('.tab').forEach(tab=>tab.addEventListener('click',()=>{document.querySelectorAll('.tab').forEach(item=>item.classList.toggle('active',item===tab));document.querySelectorAll('.panel').forEach(panel=>panel.classList.toggle('active',panel.id===`panel-${tab.dataset.panel}`));if(tab.dataset.panel==='system')getJSON('/api/observability').then(data=>{state.telemetry=data;renderSystem()})}));
$('searchForm').addEventListener('submit',async event=>{event.preventDefault();$('searchResults').innerHTML='<div class="skeleton"></div>';const data=await getJSON(`/api/search?q=${encodeURIComponent($('searchInput').value)}`);renderSearch(data.results)});
let budgetTimer;$('budgetInput').addEventListener('input',()=>{$('budgetValue').textContent=$('budgetInput').value;clearTimeout(budgetTimer);budgetTimer=setTimeout(loadReport,250)});
$('candidateDialog').querySelector('.dialog-close').onclick=()=>$('candidateDialog').close();
init();
