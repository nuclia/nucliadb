{{/*
Expand the name of the chart.
*/}}
{{- define "common.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "common.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "common.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Defines the internal kubernetes address to Atlantis
*/}}
{{- define "common.url" -}}
{{ template "common.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.service.port }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "common.labels" -}}
app: {{ template "common.name" . }}
app.kubernetes.io/name: {{ template "common.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
chart: {{ template "common.chart" . }}
helm.sh/chart: {{ template "common.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.commonLabels}}
{{ toYaml .Values.commonLabels }}
{{- end }}
{{- end -}}

{{/*
Render a list of environment variables from a map of key-value pairs.

Possible values for the map are:
- string: the value is rendered as a string
    example: 
      key: value
- map: the value is rendered as a YAML object
    example: 
      key:
        valueFrom:
          secretKeyRef:
            name: secret-name
            key: secret-key
*/}}
{{- define "toEnv" }}
{{- range $key, $value := . }}
    {{- if kindIs "string" $value }}
- name: "{{ $key }}"
  value: {{ tpl $value $ | quote }}
    {{- else if kindIs "map" $value}}
- name: "{{ $key }}"
{{ $value | toYaml | indent 2}}
    {{- end }}
    {{- end }}
{{- end }}
