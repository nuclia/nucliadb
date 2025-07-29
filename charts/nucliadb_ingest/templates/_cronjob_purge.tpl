{{ define "purge.cronjob" }}
kind: CronJob
apiVersion: batch/v1
metadata:
  name: "{{ .cronname }}"
  labels:
    app: "{{ .Chart.Name }}"
    version: "{{ .Values.hash }}"
    chart: "{{ .Chart.Name }}"
    release: "{{ .Release.Name }}"
    heritage: "{{ .Release.Service }}"
spec:
  schedule: "{{ .schedule }}"
  jobTemplate:
    metadata:
      name: "{{ .cronname }}"
      labels:
        app: {{ .Chart.Name }}
        role: cronjobs
        version: "{{ .Values.hash }}"
        chart: "{{ .Chart.Name }}"
        release: "{{ .Release.Name }}"
    spec:
      backoffLimit: 0
      template:
        metadata:
          labels:
            app: "{{ .Chart.Name }}"
            metrics: "enabled"
            role: cronjobs
            version: "{{ .Values.hash }}"
            release: "{{ .Release.Name }}"
          {{- if or (hasKey .Values "extra_pod_annotations") (hasKey .Values "extra_cronjob_pod_annotations") }}
          annotations:
            {{- if hasKey .Values "extra_pod_annotations" }}
{{ toYaml .Values.extra_pod_annotations | indent 12 }}
            {{- end }}
            {{- if hasKey .Values "extra_cronjob_pod_annotations" }}
{{ toYaml .Values.extra_cronjob_pod_annotations | indent 12 }}
            {{- end }}
          {{- end }}
        spec:
          serviceAccountName: {{ ((.Values.purgeCron).serviceAccount) | default "default" }}
          nodeSelector:
{{ toYaml .Values.nodeSelector | indent 12 }}
          topologySpreadConstraints:
{{ toYaml .Values.topologySpreadConstraints | indent 12 }}
          affinity:
{{ toYaml .Values.affinity | indent 12 }}
          tolerations:
{{ toYaml .Values.tolerations | indent 12 }}
          dnsPolicy: ClusterFirst
{{- with .Values.priorityClassName }}
          priorityClassName: {{ . }}
{{- end }}
          restartPolicy: Never
          containers:
          - name: "{{ .cronname }}"
            image: "{{ .Values.containerRegistry }}/{{ .Values.image }}"
            envFrom:
              - configMapRef:
                  name: {{ .Release.Name }}-config
              - secretRef:
                  name: {{ .Release.Name }}-config
              {{- if .Values.envFrom }}
              {{- toYaml .Values.envFrom | nindent 14 }}
              {{- end }}
            env:
              - name: VERSION
                valueFrom:
                  fieldRef:
                    fieldPath: metadata.labels['version']
              {{- include "toEnv" .Values.env | indent 14 }}
            imagePullPolicy: Always
            command: ["{{ .command }}"]
{{ end }}