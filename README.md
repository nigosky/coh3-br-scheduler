# CoH3 BR Scheduler

Agendador público minimalista (`schedule` cron) que dispara via `workflow_dispatch` o monitor privado [`nigosky/coh3-br-tracker`](https://github.com/nigosky/coh3-br-tracker) a cada 15 minutos nos horários ativos do Brasil, mantendo o código, banco de dados e Webhooks do projeto principal em repositório privado.

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
3. Pronto! O agendador disparará automaticamente nos minutos `:07`, `:22`, `:37`, `:52` (12h–02h BRT) e 4x na madrugada/manhã, totalizando 60 ciclos/dia (~30 min/mês de cota no repositório privado, bem abaixo dos 2.000 min/mês gratuitos).
