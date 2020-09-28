{{- define "app.name" -}}
{{- $.Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "app.fullname" -}}
{{- printf "%s-%s" .Chart.Name (lower .Release.Name) | trunc 63 | replace "_" "-" | trimSuffix "-" -}}
{{- end -}}

{{/* Generate basic release labels */}}
{{- define "app.labels" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: pipeline-kafka-backup
app.name: {{ .Chart.Name }}
app.release: {{ .Release.Name }}
app.version: {{ .Chart.AppVersion | quote }}
{{- end }}
