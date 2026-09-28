{{- define "tuba.labels" -}}
app.kubernetes.io/name: tuba
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: tuba
{{- end }}

{{- define "tuba.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "tuba.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "tuba.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "tuba.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tuba.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "tuba.secretName" -}}
{{- if .Values.vault.enabled -}}
{{- .Values.vault.secretName -}}
{{- else -}}
{{- .Values.secrets.existingSecret -}}
{{- end -}}
{{- end }}

{{- define "tuba.commonEnv" -}}
KAFKA_BROKERS: {{ .Values.common.kafka.brokers | quote }}
KAFKA_RAW_TOPIC: {{ .Values.common.kafka.rawTopic | quote }}
KAFKA_QUARANTINE_TOPIC: {{ .Values.common.kafka.quarantineTopic | quote }}
KAFKA_EVENTS_TOPIC: {{ .Values.common.kafka.eventsTopic | quote }}
KAFKA_ANALYSIS_RESULTS_TOPIC: {{ .Values.common.kafka.analysisResultsTopic | quote }}
KAFKA_DLQ_TOPIC: {{ .Values.common.kafka.dlqTopic | quote }}
KAFKA_SECURITY_PROTOCOL: {{ .Values.common.kafka.securityProtocol | quote }}
KAFKA_SASL_MECHANISM: {{ .Values.common.kafka.saslMechanism | quote }}
ES_URL: {{ .Values.common.elasticsearch.url | quote }}
TUBA_ORGANIZATION_ID: {{ .Values.common.organization | quote }}
TUBA_NAMESPACE: {{ .Values.common.namespace | quote }}
OTEL_EXPORTER_OTLP_ENDPOINT: {{ .Values.common.otel.endpoint | quote }}
{{- end }}
