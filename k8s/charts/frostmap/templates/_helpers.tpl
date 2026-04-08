{{/*
Expand the name of the chart.
*/}}
{{- define "frostmap.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name. Truncated to fit the 63-char DNS label limit.
*/}}
{{- define "frostmap.fullname" -}}
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

{{- define "frostmap.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every object.
*/}}
{{- define "frostmap.labels" -}}
helm.sh/chart: {{ include "frostmap.chart" . }}
{{ include "frostmap.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "frostmap.selectorLabels" -}}
app.kubernetes.io/name: {{ include "frostmap.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-component names. Defaults to <fullname>-control-plane / -node-agent.
*/}}
{{- define "frostmap.controlPlane.name" -}}
{{- printf "%s-control-plane" (include "frostmap.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "frostmap.nodeAgent.name" -}}
{{- printf "%s-node-agent" (include "frostmap.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Resolve the container image. Tag defaults to Chart.AppVersion.
*/}}
{{- define "frostmap.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Builder image used by the control-plane to launch build Jobs. Defaults to
the same image the chart is installing.
*/}}
{{- define "frostmap.builderImage" -}}
{{- if .Values.controlPlane.builderImage -}}
{{- .Values.controlPlane.builderImage -}}
{{- else -}}
{{- include "frostmap.image" . -}}
{{- end -}}
{{- end -}}

{{/*
Default control-plane URL for the node-agent: in-cluster service DNS.
*/}}
{{- define "frostmap.controlPlane.url" -}}
{{- if .Values.nodeAgent.controlPlaneURL -}}
{{- .Values.nodeAgent.controlPlaneURL -}}
{{- else -}}
{{- printf "http://%s.%s.svc:%d" (include "frostmap.controlPlane.name" .) .Release.Namespace (int .Values.service.port) -}}
{{- end -}}
{{- end -}}

{{/*
Validation: storageClass is required.
*/}}
{{- define "frostmap.requireStorageClass" -}}
{{- if not .Values.controlPlane.storageClass -}}
{{- fail "controlPlane.storageClass is required: set it to the StorageClass used for build PVCs (e.g. 'standard' for KIND, 'hyperdisk-ml' for GKE)" -}}
{{- end -}}
{{- .Values.controlPlane.storageClass -}}
{{- end -}}

{{/*
Validation: leader-election must be on when running >1 replica.
*/}}
{{- define "frostmap.checkLeaderElection" -}}
{{- if and (gt (int .Values.controlPlane.replicas) 1) (not .Values.controlPlane.leaderElection) -}}
{{- fail "controlPlane.leaderElection must be true when controlPlane.replicas > 1" -}}
{{- end -}}
{{- end -}}
