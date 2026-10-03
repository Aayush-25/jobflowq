# Externalize docker-compose Credentials to .env

## Context

`docker-compose.yml` currently hardcodes `POSTGRES_DB`, `POSTGRES_USER`,
`POSTGRES_PASSWORD` (on the `postgres` service) and
`SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD` (on the `app`
service) directly in source. A background security review flagged this.
This spec moves those five values into a gitignored `.env` file, with a
committed `.env.example` template, referenced from `docker-compose.yml`
via `${VAR_NAME}` substitution.

## Goals

- Create `.env` with the current real values for all 5 variables.
- Create `.env.example` with placeholder values, safe to commit.
- Update `docker-compose.yml` to reference all 5 via `${VAR_NAME}`.
- Add `.env` to `.gitignore` (not `.env.example`).
- Also re-point the `postgres` healthcheck (`pg_isready -U admin -d
  jobflowq`) and the `app` service's `SPRING_DATASOURCE_URL` (`jdbc:
  postgresql://postgres:5432/jobflowq`) at `${POSTGRES_USER}`/
  `${POSTGRES_DB}` instead of the same values hardcoded a second time —
  otherwise the healthcheck/URL could silently drift from the externalized
  credentials.

## Non-goals

- No change to `application.properties` or any Java code.
- No Docker secrets / vault integration — a gitignored `.env` file matches
  the project's current local-dev-only security posture.
- No change to the actual credential values — same `admin`/`password`/
  `jobflowq`, just relocated.

## Design

### 1. `.env` (new, gitignored)
```
POSTGRES_DB=jobflowq
POSTGRES_USER=admin
POSTGRES_PASSWORD=password
SPRING_DATASOURCE_USERNAME=admin
SPRING_DATASOURCE_PASSWORD=password
```

### 2. `.env.example` (new, committed)
```
POSTGRES_DB=your_db_name
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
SPRING_DATASOURCE_USERNAME=your_db_user
SPRING_DATASOURCE_PASSWORD=your_db_password
```

### 3. `docker-compose.yml`

`postgres` service:
```yaml
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
```

`app` service:
```yaml
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/${POSTGRES_DB}
      SPRING_DATASOURCE_USERNAME: ${SPRING_DATASOURCE_USERNAME}
      SPRING_DATASOURCE_PASSWORD: ${SPRING_DATASOURCE_PASSWORD}
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

Docker Compose automatically loads `.env` from the same directory as
`docker-compose.yml` for `${VAR}` substitution at parse time — no
`env_file:` directive is needed for this.

### 4. `.gitignore`

Add a line for `.env` (not `.env.example`).

## Testing

- `docker compose config` resolves all `${VAR}` references with no
  warnings about unset variables.
- `docker-compose up --build` brings up all 3 services healthy, exactly as
  before — confirms the substituted values match the previous hardcoded
  ones.
- `git status` shows `.env` as untracked/ignored, `.env.example` as a new
  trackable file.
