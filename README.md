# CoH3 BR Scheduler (Smart Pre-Check)

Agendador público inteligente (`schedule` cron + pré-checagem via `precheck.py` stdlib-only) que consulta a API pública da Relic das **44 contas Steam monitoradas** a cada **5 minutos** nos horários ativos do Brasil (12h–02h BRT) e **só dispara `workflow_dispatch` no repositório privado [`nigosky/coh3-br-tracker`](https://github.com/nigosky/coh3-br-tracker) quando há partida nova** (ranqueada ou customizada), janela do Ranking Diário (`00:01 BRT`), janela do Resumo Semanal (`segunda-feira 00:05 BRT`) ou heartbeat de segurança de 3h.

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
