# CoH3 BR Scheduler (Smart Pre-Check)

Agendador público inteligente (`Self-Relay` 5 em 5 min + pré-checagem via `precheck.py` stdlib-only) que consulta a API pública da Relic das **45 contas Steam monitoradas** 24h/dia, cruza sinais somente-leitura do **Radar de Lobby dos Veteranos** (`nigosky/coh3-br-lobby-radar` + `getPersonalStat`) para rechecagem prioritária de quem acabou de jogar CoH3, e **só dispara `workflow_dispatch` no repositório privado [`nigosky/coh3-br-tracker`](https://github.com/nigosky/coh3-br-tracker) quando há partida nova** (ranqueada ou customizada), janela do Resumo Semanal (`segunda-feira 00:05 BRT`) ou heartbeat de segurança de 3h. *(O Ranking Unificado BR agora roda em projeto independente.)*

- **Minutos no repositório público (`coh3-br-scheduler`):** gratuitos e ilimitados no GitHub Actions.
- **Minutos no repositório privado (`coh3-br-tracker`):** economia de ~70–85% da cota mensal gratuita (2.000 min/mês), com detecção de partidas em até **5 minutos**!

## Configuração única do Secret `TRACKER_DISPATCH_TOKEN`

1. Acesse **GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token** ([link direto](https://github.com/settings/personal-access-tokens/new)):
   - **Token name:** `coh3-br-scheduler-dispatch`
   - **Expiration:** `1 year` (ou conforme preferir)
   - **Repository access:** marque **Only select repositories** e selecione **`nigosky/coh3-br-tracker`**
   - **Permissions → Repository permissions:**
     - **Actions:** `Read and write` *(única permissão necessária — `Metadata: Read-only` é selecionada automaticamente)*
   - Clique em **Generate token** e copie o token gerado (`github_pat_...`).
2. Salve o token nos Secrets deste repositório público ([link direto](https://github.com/nigosky/coh3-br-scheduler/settings/secrets/actions/new)):
   - **Name:** `TRACKER_DISPATCH_TOKEN`
   - **Secret:** cole o token `github_pat_...` e clique em **Add secret**.
