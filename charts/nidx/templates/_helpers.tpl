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
