{{/* SPDX-License-Identifier: Apache-2.0 */ -}}
{{/*
Expand the name of the chart.
*/}}
{{- define "mcfreeze.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name. Truncated to fit the 63-char DNS label limit.
*/}}
{{- define "mcfreeze.fullname" -}}
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

{{- define "mcfreeze.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every object.
*/}}
{{- define "mcfreeze.labels" -}}
helm.sh/chart: {{ include "mcfreeze.chart" . }}
{{ include "mcfreeze.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "mcfreeze.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mcfreeze.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-component names. Defaults to <fullname>-control-plane / -node-agent.
*/}}
{{- define "mcfreeze.controlPlane.name" -}}
{{- printf "%s-control-plane" (include "mcfreeze.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mcfreeze.nodeAgent.name" -}}
{{- printf "%s-node-agent" (include "mcfreeze.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mcfreeze.builder.name" -}}
{{- printf "%s-builder" (include "mcfreeze.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Resolve the container image. Tag defaults to Chart.AppVersion.
*/}}
{{- define "mcfreeze.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Builder image used by the control-plane to launch build Jobs. Defaults to
the same image the chart is installing.
*/}}
{{- define "mcfreeze.builderImage" -}}
{{- if .Values.controlPlane.builderImage -}}
{{- .Values.controlPlane.builderImage -}}
{{- else -}}
{{- include "mcfreeze.image" . -}}
{{- end -}}
{{- end -}}

{{/*
Default control-plane URL for the node-agent: in-cluster service DNS.
*/}}
{{- define "mcfreeze.controlPlane.url" -}}
{{- if .Values.nodeAgent.controlPlaneURL -}}
{{- .Values.nodeAgent.controlPlaneURL -}}
{{- else -}}
{{- printf "http://%s.%s.svc:%d" (include "mcfreeze.controlPlane.name" .) .Release.Namespace (int .Values.service.port) -}}
{{- end -}}
{{- end -}}

{{/*
Validation: storageClass is required.
*/}}
{{- define "mcfreeze.requireStorageClass" -}}
{{- if not .Values.controlPlane.storageClass -}}
{{- fail "controlPlane.storageClass is required: set it to the StorageClass used for build PVCs (e.g. 'standard' for KIND, 'hyperdisk-ml' for GKE)" -}}
{{- end -}}
{{- .Values.controlPlane.storageClass -}}
{{- end -}}

{{/*
Builder pod template: merge the builder SA name with user-provided overrides
and serialize to compact JSON for the --builder-pod-template flag.
*/}}
{{- define "mcfreeze.builderPodTemplate" -}}
{{- $tmpl := dict "serviceAccountName" (include "mcfreeze.builder.name" .) -}}
{{- with .Values.controlPlane.builderPodTemplate -}}
{{- $tmpl = merge . $tmpl -}}
{{- end -}}
{{- $tmpl | toJson -}}
{{- end -}}

{{/*
Validation: leader-election must be on when running >1 replica.
*/}}
{{- define "mcfreeze.checkLeaderElection" -}}
{{- if and (gt (int .Values.controlPlane.replicas) 1) (not .Values.controlPlane.leaderElection) -}}
{{- fail "controlPlane.leaderElection must be true when controlPlane.replicas > 1" -}}
{{- end -}}
{{- end -}}
