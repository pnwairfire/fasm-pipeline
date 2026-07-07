# Secrets in production (ECS / Fargate)

The container reads all configuration from **environment variables** — it doesn't
care where they come from. In production, don't ship a `.env`; have the ECS task
inject secrets from **SSM Parameter Store** or **Secrets Manager**. No code
change is needed here — this wiring lives in the ECS task definition (i.e. your
Prefect ECS work pool's base job template), not in this repo.

Split values two ways:
- **Non-secret config** (hosts, users, DB names, bucket names, table/schema
  overrides) → the task definition's `environment` block, in plaintext.
- **Secrets** (DB passwords, AWS secret keys) → the `secrets` block, pulled from
  SSM/Secrets Manager.

## 1. Store the secrets

SSM Parameter Store (SecureString) is the cheap, simple choice:

```bash
aws ssm put-parameter --name /fasm/TS_DB_PW      --type SecureString --value '...'
aws ssm put-parameter --name /fasm/AIRFIRE_DB_PW --type SecureString --value '...'
aws ssm put-parameter --name /fasm/EPA_AWS_SECRET_ACCESS_KEY --type SecureString --value '...'
```

Use **Secrets Manager** instead only if you want rotation or JSON-bundled
secrets (`aws secretsmanager create-secret ...`).

## 2. Reference them in the task definition

```jsonc
{
  "environment": [
    { "name": "TS_DB_HOST",       "value": "ts-db.example.com" },
    { "name": "TS_DB_USER",       "value": "fasm" },
    { "name": "TS_DB_DATABASE",   "value": "tileserver" },
    { "name": "AIRFIRE_DB_HOST",  "value": "airfire-db.example.com" },
    { "name": "AIRFIRE_DB_USER",  "value": "fasm" },
    { "name": "AIRFIRE_DB_DATABASE", "value": "airfire" },
    { "name": "AFE_BUCKET",       "value": "airfire-exports" },
    { "name": "EPA_BUCKET",       "value": "epa-layers" },
    { "name": "EPA_AWS_ACCESS_KEY", "value": "AKIA..." }
  ],
  "secrets": [
    { "name": "TS_DB_PW",      "valueFrom": "arn:aws:ssm:us-west-2:123456789012:parameter/fasm/TS_DB_PW" },
    { "name": "AIRFIRE_DB_PW", "valueFrom": "arn:aws:ssm:us-west-2:123456789012:parameter/fasm/AIRFIRE_DB_PW" },
    { "name": "EPA_AWS_SECRET_ACCESS_KEY", "valueFrom": "arn:aws:ssm:us-west-2:123456789012:parameter/fasm/EPA_AWS_SECRET_ACCESS_KEY" }
  ]
}
```

`name` must match the variable names the app reads (see the README env-var
tables). For Secrets Manager, `valueFrom` is the secret ARN; append `:KEY::` to
pull a single key out of a JSON secret.

## 3. Grant the execution role access

The **task execution role** (the one ECS uses to launch the container) needs to
read the referenced parameters/secrets:

```jsonc
{
  "Effect": "Allow",
  "Action": ["ssm:GetParameters"],           // or "secretsmanager:GetSecretValue"
  "Resource": "arn:aws:ssm:us-west-2:123456789012:parameter/fasm/*"
}
```

Add `kms:Decrypt` on the encrypting key only if you used a customer-managed KMS
key (the default `aws/ssm` key needs no extra grant).

## 4. Optional: use the task IAM role for S3 (drop the AirFire static keys)

`init_s3()` falls back to boto3's default credential chain when
`AWS_ACCESS_KEY` is unset — i.e. the **ECS task role**. So for the AirFire
bucket you can omit `AWS_ACCESS_KEY` / `AWS_SECRET_ACCESS_KEY` entirely and
instead grant the **task role** (not the execution role):

```jsonc
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject"],
  "Resource": "arn:aws:s3:::airfire-exports/*"
}
```

Caveat: the `EPA_AWS_*` credentials target a **cross-account** bucket, so a
single task role can't cover both. Keep the EPA keys as injected secrets (step 2)
unless you set up cross-account access.

---

For local development and validation you still use a `.env` file — see the
README and `scratch/README.md`.
