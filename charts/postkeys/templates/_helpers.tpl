{{/*
Expand the name of the chart.
*/}}
{{- define "postkeys.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "postkeys.fullname" -}}
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
{{- define "postkeys.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "postkeys.labels" -}}
helm.sh/chart: {{ include "postkeys.chart" . }}
{{ include "postkeys.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "postkeys.selectorLabels" -}}
app.kubernetes.io/name: {{ include "postkeys.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "postkeys.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "postkeys.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get the PostgreSQL secret name
*/}}
{{- define "postkeys.postgresqlSecretName" -}}
{{- if .Values.postgresql.existingSecret.name }}
{{- .Values.postgresql.existingSecret.name }}
{{- else }}
{{- include "postkeys.fullname" . }}-postgresql
{{- end }}
{{- end }}

{{/*
Get the Redis secret name
*/}}
{{- define "postkeys.redisSecretName" -}}
{{- if .Values.redis.password.existingSecret.name }}
{{- .Values.redis.password.existingSecret.name }}
{{- else if .Values.redis.password.secretName }}
{{- .Values.redis.password.secretName }}
{{- else }}
{{- include "postkeys.fullname" . }}-redis
{{- end }}
{{- end }}

{{/*
Return true if a PostgreSQL secret should be created
*/}}
{{- define "postkeys.createPostgresqlSecret" -}}
{{- if and (not .Values.postgresql.existingSecret.name) .Values.postgresql.auth.password }}
{{- true }}
{{- end }}
{{- end }}

{{/*
Return true if a Redis secret should be created from the provided value
(not via the Job-based auto-generation)
*/}}
{{- define "postkeys.createRedisSecret" -}}
{{- if and (not .Values.redis.password.create) (not .Values.redis.password.existingSecret.name) .Values.redis.password.value }}
{{- true }}
{{- end }}
{{- end }}
