/* =====================================================================
   ADAPTADOR SUPABASE
   O painel foi escrito para o banco do artifact do Claude (claude.use("db")).
   Este adaptador oferece a mesma interface (collection/doc/onSnapshot/set/
   update/delete) usando o Supabase, mais login por e-mail. Assim o código do
   painel continua igual nas duas versões.
   ===================================================================== */
(function(){
  const CFG = window.CARTEIRA_CONFIG || {};
  const configurado = CFG.supabaseUrl && CFG.supabaseAnonKey && !/SEU-PROJETO|COLE_AQUI/.test(CFG.supabaseUrl + CFG.supabaseAnonKey);
  let liberar; const pronto = new Promise(r => liberar = r);
  let sb = null, dbShim = null, userShim = null, nomesMembros = new Set();

  const $l = s => document.querySelector(s);
  function tela(html){ $l('#login').classList.remove('hidden'); $l('#loginBox').innerHTML = html; }
  function fecharTela(){ $l('#login').classList.add('hidden'); }
  const escL = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  function erroSb(e){
    const m = (e && (e.message || e.details)) || String(e || '');
    const code = /row-level security|permission denied|42501|não permitida|not found|não encontrado/i.test(m + (e && e.code)) ? 'invalid_argument'
      : /fetch|network|timeout|JWT expired/i.test(m) ? 'unavailable' : 'invalid_argument';
    return {code, message: m};
  }

  function criarDb(){
    const cache = {}, ouvintes = {}, canais = {}, carregando = {};
    const avisar = t => (ouvintes[t] || new Set()).forEach(f => { try { f(); } catch(e){ console.error(e); } });
    async function carregar(t){
      if (carregando[t]) return carregando[t];
      carregando[t] = (async () => {
        const linhas = []; let de = 0;
        while (true) {   // pagina de 1000 em 1000
          const {data, error} = await sb.from(t).select('id,data').order('id').range(de, de + 999);
          if (error) throw erroSb(error);
          linhas.push(...data); if (data.length < 1000) break; de += 1000;
        }
        cache[t] = new Map(linhas.map(r => [r.id, r]));
      })();
      try { await carregando[t]; } finally { carregando[t] = null; }
    }
    function assinar(t){
      if (canais[t]) return;
      canais[t] = sb.channel('rt-' + t)
        .on('postgres_changes', {event: '*', schema: 'public', table: t}, p => {
          if (!cache[t]) return;
          if (p.eventType === 'DELETE') cache[t].delete(p.old && p.old.id);
          else cache[t].set(p.new.id, {id: p.new.id, data: p.new.data});
          avisar(t);
        })
        .subscribe(status => { if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') setTimeout(() => carregar(t).then(() => avisar(t)).catch(() => {}), 5000); });
    }
    const meta = {fromCache: false, hasPendingWrites: false};
    const docSnap = (id, r) => ({id, exists: !!r, data: () => r ? r.data : undefined, metadata: meta});
    function querySnap(t, filtro){
      const rows = [...cache[t].values()].filter(r => !filtro || filtro(r)).sort((a, b) => a.id < b.id ? -1 : 1);
      return {docs: rows.map(r => docSnap(r.id, r)), size: rows.length, empty: !rows.length, metadata: meta, docChanges: () => []};
    }
    function ouvir(t, f){ (ouvintes[t] = ouvintes[t] || new Set()).add(f); return () => ouvintes[t].delete(f); }
    function iniciar(t, disparar, err){
      (cache[t] ? Promise.resolve() : carregar(t)).then(() => { assinar(t); disparar(); }).catch(e => err && err(e));
    }
    function query(t, filtro){
      return {
        where(campo, op, valor){
          if (op !== '==') throw new TypeError('Somente "==" é suportado');
          return query(t, r => (!filtro || filtro(r)) && r.data && r.data[campo] === valor);
        },
        orderBy(){ return query(t, filtro); }, limit(){ return query(t, filtro); },
        async get(){ await carregar(t); return querySnap(t, filtro); },
        onSnapshot(next, err){
          let vivo = true; const disparar = () => { if (vivo && cache[t]) next(querySnap(t, filtro)); };
          const off = ouvir(t, disparar); iniciar(t, disparar, err);
          return () => { vivo = false; off(); };
        }
      };
    }
    function gravarLocal(t, id, data){ if (cache[t]) { if (data === null) cache[t].delete(id); else cache[t].set(id, {id, data}); avisar(t); } }
    function docRef(t, id){
      return {
        id, path: t + '/' + id,
        async get(){ const {data, error} = await sb.from(t).select('id,data').eq('id', id).maybeSingle(); if (error) throw erroSb(error); return docSnap(id, data); },
        async set(d){ const {error} = await sb.from(t).upsert({id, data: d}); if (error) throw erroSb(error); gravarLocal(t, id, d); },
        async update(p){ const {data, error} = await sb.rpc('atualizar_doc', {tabela: t, doc_id: id, patch: p}); if (error) throw erroSb(error); gravarLocal(t, id, data); },
        async delete(){ const {error} = await sb.from(t).delete().eq('id', id); if (error) throw erroSb(error); gravarLocal(t, id, null); },
        onSnapshot(next, err){
          let vivo = true; const disparar = () => { if (vivo && cache[t]) next(docSnap(id, cache[t].get(id))); };
          const off = ouvir(t, disparar); iniciar(t, disparar, err);
          return () => { vivo = false; off(); };
        }
      };
    }
    const novoId = () => 'x' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
    return {
      collection: nome => Object.assign(query(nome), {path: nome, doc: id => docRef(nome, id || novoId()),
        add: async d => { const r = docRef(nome, novoId()); await r.set(d); return r; }}),
      doc: caminho => { const [t, id] = caminho.split('/'); return docRef(t, id); }
    };
  }

  const dlShim = {
    async save({filename, data}){
      const blob = data instanceof Blob ? data : new Blob([data], {type: /\.json$/i.test(filename) ? 'application/json' : 'text/csv;charset=utf-8'});
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = filename;
      document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
      return {status: 'saved'};
    }
  };

  async function entrar(sessao){
    const email = sessao.user.email;
    const {data: membros, error} = await sb.from('membros').select('email,nome,ativo');
    const eu = (membros || []).find(m => m.email.toLowerCase() === email.toLowerCase() && m.ativo);
    if (error || !eu) {
      tela(`<h2>Sem acesso</h2><p>O e-mail <b>${escL(email)}</b> entrou, mas não está na lista de membros do painel.</p><p class="hint">Peça para a Mariane incluir você na tabela <b>membros</b> do Supabase.</p><button class="btn ghost" id="lgSair">Sair e usar outro e-mail</button>`);
      $l('#lgSair').onclick = async () => { await sb.auth.signOut(); location.reload(); };
      return;
    }
    nomesMembros = new Set(membros.map(m => m.nome));
    userShim = {
      id: async () => eu.nome, me: async () => ({id: eu.nome, name: eu.nome}),
      profiles: async ids => Object.fromEntries(ids.map(i => [i, {name: nomesMembros.has(i) ? i : ''}])),
      isOwner: () => false, canEdit: () => true, can: () => true
    };
    dbShim = criarDb();
    const bx = $l('#usuarioBox'); if (bx) { bx.classList.remove('hidden'); $l('#usuarioNome').textContent = eu.nome; }
    fecharTela(); liberar(true);
  }

  function telaLogin(msg){
    tela(`<div class="lg-mark">C</div><h2>Carteira Geral</h2><p>Entre com o seu e-mail de trabalho. Você vai receber um <b>link de acesso</b> (não precisa de senha).</p>
      <form id="lgForm"><input type="email" id="lgEmail" required placeholder="seu@email.com.br" autocomplete="email"><button class="btn primary" type="submit" id="lgBtn">Enviar link de acesso</button></form>
      <p class="hint" id="lgMsg">${msg || ''}</p>`);
    $l('#lgForm').onsubmit = async ev => {
      ev.preventDefault(); const email = $l('#lgEmail').value.trim(); $l('#lgBtn').disabled = true;
      const {error} = await sb.auth.signInWithOtp({email, options: {emailRedirectTo: location.origin + location.pathname, shouldCreateUser: false}});
      $l('#lgBtn').disabled = false;
      $l('#lgMsg').innerHTML = error
        ? `<span style="color:#b83a2e">Não foi possível enviar: ${escL(/signups not allowed|not found|Signups/i.test(error.message) ? 'este e-mail não foi convidado.' : error.message)}</span>`
        : `Pronto! Abra o e-mail enviado para <b>${escL(email)}</b> e clique no link. Pode fechar esta aba.`;
    };
  }

  async function iniciar(){
    if (!configurado) { tela(`<h2>Configuração pendente</h2><p>Preencha <b>config.js</b> com a URL e a chave "anon" do seu projeto Supabase.</p>`); return; }
    if (!window.supabase) { tela(`<h2>Sem conexão</h2><p>Não consegui carregar a biblioteca do Supabase. Verifique a internet e recarregue.</p>`); return; }
    sb = window.supabase.createClient(CFG.supabaseUrl, CFG.supabaseAnonKey, {auth: {persistSession: true, detectSessionInUrl: true}});
    const {data} = await sb.auth.getSession();
    if (data.session) return entrar(data.session);
    telaLogin();
    sb.auth.onAuthStateChange((evento, sessao) => { if (evento === 'SIGNED_IN' && sessao && !dbShim) entrar(sessao); });
  }

  window.sairDoPainel = async () => { if (sb) await sb.auth.signOut(); location.reload(); };
  window.claude = {
    use: async nome => {
      await pronto;
      return nome === 'db' ? dbShim : nome === 'user' ? userShim : nome === 'downloads' ? dlShim : null;
    }
  };
  document.addEventListener('DOMContentLoaded', iniciar);
})();
