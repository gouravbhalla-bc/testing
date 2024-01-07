{{/*
Expand the name of the chart.
*/}}
{{- define "ace-services.name" -}}
{{- default .Release.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ace-services.fullname" -}}
{{- printf "%s" (include "ace-services.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ace-services.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ace-services.labels" -}}
helm.sh/chart: {{ include "ace-services.chart" . }}
{{ include "ace-services.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app: {{ .Chart.Name }}
mode: {{ .Values.mode }}
version: {{ default .Chart.AppVersion .Values.image.tag | quote }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ace-services.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ace-services.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
