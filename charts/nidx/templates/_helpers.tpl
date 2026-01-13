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

{{/*
Render a ServiceAccount resource.
Usage: {{ include "serviceAccount" (dict "component" "ingest" "appLabel" "ingest" "serviceAccount" .Values.ingest.serviceAccount "Chart" .Chart "Release" .Release) }}
Parameters:
- component: the component name (used for conditional creation)
- appLabel: the value for the app label
- serviceAccount: the serviceAccount configuration from values
- Chart: the Chart object
- Release: the Release object
*/}}
{{- define "serviceAccount" }}
{{- if kindIs "map" .serviceAccount }}
{{- if .serviceAccount.create }}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .serviceAccount.name }}
  labels:
    app: {{ .appLabel }}
    version: "{{ .Chart.Version | replace "+" "_" }}"
    chart: "{{ .Chart.Name }}"
    release: "{{ .Release.Name }}"
    heritage: "{{ .Release.Service }}"
    {{- with .serviceAccount.labels }}
      {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .serviceAccount.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
---
{{- end }}
{{- end }}
{{- end }}
