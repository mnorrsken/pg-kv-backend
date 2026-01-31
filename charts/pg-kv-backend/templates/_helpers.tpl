{{/*
Expand the name of the chart.
*/}}
{{- define "pg-kv-backend.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pg-kv-backend.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pg-kv-backend.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "pg-kv-backend.labels" -}}
helm.sh/chart: {{ include "pg-kv-backend.chart" . }}
{{ include "pg-kv-backend.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "pg-kv-backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pg-kv-backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "pg-kv-backend.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pg-kv-backend.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get the PostgreSQL secret name
*/}}
{{- define "pg-kv-backend.postgresqlSecretName" -}}
{{- if .Values.postgresql.existingSecret.name }}
{{- .Values.postgresql.existingSecret.name }}
{{- else }}
{{- include "pg-kv-backend.fullname" . }}-postgresql
{{- end }}
{{- end }}

{{/*
Get the Redis secret name
*/}}
{{- define "pg-kv-backend.redisSecretName" -}}
{{- if .Values.redis.password.existingSecret.name }}
{{- .Values.redis.password.existingSecret.name }}
{{- else if .Values.redis.password.secretName }}
{{- .Values.redis.password.secretName }}
{{- else }}
{{- include "pg-kv-backend.fullname" . }}-redis
{{- end }}
{{- end }}

{{/*
Return true if a PostgreSQL secret should be created
*/}}
{{- define "pg-kv-backend.createPostgresqlSecret" -}}
{{- if and (not .Values.postgresql.existingSecret.name) .Values.postgresql.auth.password }}
{{- true }}
{{- end }}
{{- end }}

{{/*
Return true if a Redis secret should be created from the provided value
(not via the Job-based auto-generation)
*/}}
{{- define "pg-kv-backend.createRedisSecret" -}}
{{- if and (not .Values.redis.password.create) (not .Values.redis.password.existingSecret.name) .Values.redis.password.value }}
{{- true }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
