{{/* Render MarkLogic endpoints and credentials for all NucliaDB workloads. */}}
{{- define "marklogicEnv" -}}
{{- $server := .Values.marklogic.server -}}
{{- $secretName := default "marklogic-admin" $server.authSecretName -}}
{{- $uri := printf "http://%s:8000" $server.host -}}
{{- $metaServer := dict "uri" $uri "admin_port" $server.adminPort "init_port" $server.initPort | toJson -}}
{{- $dataServer := dict "id" $server.serverId "uri" $uri "admin_port" $server.adminPort "init_port" $server.initPort -}}
- name: MARKLOGIC_META_SERVER
  value: {{ $metaServer | quote }}
- name: MARKLOGIC_DATA_SERVERS
  value: {{ list $dataServer | toJson | quote }}
- name: MARKLOGIC_META_SERVER__USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $secretName | quote }}
      key: username
- name: MARKLOGIC_META_SERVER__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secretName | quote }}
      key: password
- name: MARKLOGIC_DATA_SERVER_0_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $secretName | quote }}
      key: username
- name: MARKLOGIC_DATA_SERVER_0_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secretName | quote }}
      key: password
{{- end }}