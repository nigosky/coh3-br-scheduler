# CoH3 BR Scheduler (Smart Pre-Check)

Agendador público inteligente (`Self-Relay` 5 em 5 min + pré-checagem via `precheck.py` stdlib-only) que consulta a API pública da Relic das contas Steam monitoradas 24h/dia, cruza sinais somente-leitura do **Radar de Lobby dos Veteranos** (`nigosky/coh3-br-lobby-radar` + `getPersonalStat`) para rechecagem prioritária de quem acabou de jogar CoH3, e dispara `workflow_dispatch` nos repositórios privados:

- **[`nigosky/coh3-br-tracker`](https://github.com/nigosky/coh3-br-tracker)** (`coh3_monitor.yml`): somente quando há partida nova (ranqueada ou customizada), confirmação de Battlegroups, janela do Resumo Semanal (`segunda-feira >= 00:05 BRT`) ou heartbeat de segurança de 3h.
- **[`nigosky/coh3-br-ranking`](https://github.com/nigosky/coh3-br-ranking)**:
  - **Boletim Diário + Jogo do Dia** (`boletim_diario.yml`): todo dia a partir das `08:03 BRT` (`last_ranking_daily_date`).
  - **Ranking Unificado + Times 2v2** (`ranking_unificado.yml`): 1x/dia a partir das `00:02 BRT` (janela até `11:59 BRT`), com `inputs={"force_update": false}` (`last_ranking_slot`).
  - **Campeões BR por Battlegroup** (`campeoes_battlegroups.yml`): todo sábado a partir das `12:05 BRT`, com janela de recuperação até domingo `11:59 BRT` (`last_ranking_bg_slot`).

- **Minutos no repositório público (`coh3-br-scheduler`):** gratuitos e ilimitados no GitHub Actions.
- **Minutos nos repositórios privados:** economia de ~70–85% da cota mensal gratuita (2.000 min/mês), com detecção e agendamentos pontuais em até **5 minutos**!

## Configuração dos Secrets (`TRACKER_DISPATCH_TOKEN` e opcional `RANKING_DISPATCH_TOKEN`)

1. Acesse **GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token** ([link direto](https://github.com/settings/personal-access-tokens/new)):
   - **Token name:** `coh3-br-scheduler-dispatch`
   - **Expiration:** `1 year` (ou conforme preferir)
   - **Repository access:** marque **Only select repositories** e selecione **`nigosky/coh3-br-tracker`** e **`nigosky/coh3-br-ranking`** (ou crie um token separado para `nigosky/coh3-br-ranking`)
   - **Permissions → Repository permissions:**
     - **Actions:** `Read and write` *(única permissão necessária — `Metadata: Read-only` é selecionada automaticamente)*
   - Clique em **Generate token** e copie o token gerado (`github_pat_...`).
2. Salve o token nos Secrets deste repositório público ([link direto](https://github.com/nigosky/coh3-br-scheduler/settings/secrets/actions/new)):
   - **`TRACKER_DISPATCH_TOKEN`** *(obrigatório)*: usado para `coh3-br-tracker` e como fallback automático para `coh3-br-ranking` caso `RANKING_DISPATCH_TOKEN` não esteja definido.
   - **`RANKING_DISPATCH_TOKEN`** *(opcional)*: se configurado, é usado com prioridade exclusiva para os workflows de `nigosky/coh3-br-ranking`.

## Auto-Recuperação (`watchdog.yml`) e Segurança

- **Watchdog Dead-Man's Switch (`.github/workflows/watchdog.yml`):** roda isolado a cada 30 min (`17,47 * * * *`) sem checkout e sem PATs (`permissions: actions: write`), religando automaticamente `scheduler.yml` caso uma sessão seja cancelada pela infraestrutura do GitHub Actions. Para pausar a cadeia intencionalmente, defina a variável de repositório `SCHEDULER_AUTO_RELAY_ENABLED=false`.
- **Performance e Hardening:** pool HTTP/1.1 Keep-Alive stdlib com TLS 1.2+ (reduz ciclos de 48 contas para ~3s sem handshakes redundantes), cache ETag/`304 Not Modified`, circuit breaker de 30 min para quedas da Relic, `actions/checkout` fixado por SHA com `persist-credentials: false`, push de `state.json` com `GIT_ASKPASS` efêmero + merge JSON monotônico (`--merge-state`), bloqueio de proxy/redirect/Gzip-bomb/AST-DoS e gate anti-vazamento (`grep -IlEi`).
