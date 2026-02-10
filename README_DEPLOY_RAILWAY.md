# Deploy na Railway (passo a passo)

Resumo rápido
- Este repositório tem dois Dockerfiles: o backend (raiz) e o frontend em `wi-front/`.
- A Railway consegue buildar serviços diretamente a partir do `Dockerfile` do repositório.

Passos (UI do Railway — método mais simples)
1. Acesse https://railway.app e conecte sua conta GitHub/GitLab.
2. Crie um novo projeto e selecione "Deploy from GitHub".
3. Escolha este repositório `wi_motos`.
4. Adicione 3 serviços (ou apenas 2 se não usar `worker` separado):
   - `backend`: build a partir do `Dockerfile` na raiz (tipo Docker).
   - `worker` (opcional): build a partir do mesmo `Dockerfile` se quiser processar filas/background jobs.
   - `frontend`: build a partir de `wi-front/Dockerfile` (serve `dist` via nginx).
5. Configure variáveis de ambiente no painel do serviço `backend` (veja abaixo).
6. Deploy — Railway fará build e rollout automático.

Variáveis de ambiente essenciais
- `DATABASE_URL`: postgresql://USER:PASS@HOST:PORT/DBNAME (Railway fornece se criar um addon Postgres)
- `JWT_SECRET`: segredo para gerar tokens JWT
- `JWT_EXPIRES_MINUTES`: tempo de expiração (opcional)
- `FRONTEND_URLS` ou `ALLOWED_ORIGINS`: origens permitidas para CORS (ex: https://app.seudominio.com)
- `UPLOAD_ROOT`: caminho relativo para armazenamento local (opcional) — recomendamos usar S3 em produção

Blibsend / integrações (copie do `docker-compose.yml` ou do seu `.env`)
- `BLIBSEND_BASE_URL`, `BLIBSEND_SESSION_TOKEN`, `BLIBSEND_CLIENT_ID`, `BLIBSEND_CLIENT_SECRET`, `BLIBSEND_DEFAULT_TO`, `BLIBSEND_GROUP_TO`

S3 (opcional — recomendado para uploads)
- `S3_BUCKET`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

Como testar localmente com Docker (rápido)
1. Backend:
```bash
docker build -t wi-moto-backend .
docker run -e PORT=8000 -e DATABASE_URL="postgresql+psycopg2://postgres:postgres@host:5432/moto_store" -p 8000:8000 wi-moto-backend
```

2. Frontend:
```bash
docker build -t wi-moto-frontend -f wi-front/Dockerfile wi-front
docker run -p 8080:80 wi-moto-frontend
```

Notas e recomendações
- Substitua armazenamento local (`uploads/`) por S3 para que o ambiente seja stateless.
- Nunca deixe `JWT_SECRET` ou credenciais hardcoded no repositório.
- Ajuste `FRONTEND_URLS` para o domínio final do frontend em produção.
- Railway lida com SSL/domínio automaticamente quando você aponta um domínio customizado.

Se quiser, eu posso:
- Gerar um `railway.json`/`railway.toml` mais detalhado (já adicionei um exemplo `railway.toml`).
- Substituir armazenamento local por S3 usando as funções já presentes em `app/infra/storage_s3.py`.
