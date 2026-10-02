"""Versioned feature, detection, and model registry loading."""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
DEFAULT_REGISTRY_PATH = Path(__file__).resolve().parent / "registry" / "analysis-registry.json"

# F08: the algorithm whitelist of the single authoritative scheduling entry
# (the Python analysis worker). Every detection scenario must be implemented
# by exactly one authoritative function in tuba_analysis.detection — the Go
# side mirrors are diagnostic/reference only and have no emission path.
ALGORITHM_FAILURE_THEN_SUCCESS = "failure_then_success"
ALGORITHM_FAILURE_BURST = "failure_burst"
ALGORITHM_BASELINE_DEVIATION = "baseline_deviation"
SUPPORTED_ALGORITHMS = {
    ALGORITHM_FAILURE_THEN_SUCCESS,
    ALGORITHM_FAILURE_BURST,
    ALGORITHM_BASELINE_DEVIATION,
}

# Default cadence of the baseline training job (periodic, design baseline §6:
# training is a scheduled task reading persisted feature samples).
DEFAULT_TRAINING_INTERVAL_SECONDS = 86400


class RegistryError(ValueError):
    """Raised when the analysis registry is invalid."""


@dataclass(frozen=True)
class RuleDefinition:
    id: str
    version: str
    algorithm: str
    feature_id: str
    severity: str
    threshold: int | None
    lookback_seconds: int
    window_seconds: int
    allowed_lateness_seconds: int
    generation: str = "g1"
    model_id: str | None = None
    z_threshold: float | None = None


@dataclass(frozen=True)
class ModelDefinition:
    """Baseline model lineage plus its scheduled training configuration."""

    id: str
    version: str
    feature_id: str
    feature_version: str
    generation: str = "g1"
    training_interval_seconds: int = DEFAULT_TRAINING_INTERVAL_SECONDS
    min_samples: int = 100
    min_complete_days: int = 14


@dataclass(frozen=True)
class AnalysisRegistry:
    version: str
    features: dict[str, dict[str, Any]]
    rules: dict[str, RuleDefinition]
    models: dict[str, ModelDefinition]

    def rule(self, rule_id: str) -> RuleDefinition:
        try:
            return self.rules[rule_id]
        except KeyError as error:
            raise RegistryError(f"unknown detection rule: {rule_id}") from error

    def model(self, model_id: str) -> ModelDefinition:
        try:
            return self.models[model_id]
        except KeyError as error:
            raise RegistryError(f"unknown baseline model: {model_id}") from error


def _positive_integer(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RegistryError(f"{field} must be a positive integer")
    return value


def _valid_generation(value: Any, field: str) -> str:
    generation = str(value if value is not None else "g1")
    if not generation or "|" in generation:
        raise RegistryError(f"invalid generation for {field}")
    return generation


def load_registry(path: str | Path | None = None) -> AnalysisRegistry:
    registry_path = Path(path or os.getenv("TUBA_ANALYSIS_REGISTRY", DEFAULT_REGISTRY_PATH))
    try:
        document = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RegistryError(f"cannot load analysis registry: {error}") from error

    version = str(document.get("registry_version", ""))
    if not VERSION_PATTERN.fullmatch(version):
        raise RegistryError("registry_version must be semantic version")

    features: dict[str, dict[str, Any]] = {}
    for feature in document.get("features", []):
        feature_id = str(feature.get("id", ""))
        feature_version = str(feature.get("version", ""))
        if not feature_id or not VERSION_PATTERN.fullmatch(feature_version):
            raise RegistryError("feature id and semantic version are required")
        if feature_id in features:
            raise RegistryError(f"duplicate feature: {feature_id}")
        features[feature_id] = feature

    rules: dict[str, RuleDefinition] = {}
    for rule in document.get("detections", []):
        rule_id = str(rule.get("id", ""))
        rule_version = str(rule.get("version", ""))
        algorithm = str(rule.get("algorithm", ""))
        feature_id = str(rule.get("feature_id", ""))
        severity = str(rule.get("severity", ""))
        parameters = rule.get("parameters", {})
        if not rule_id or not VERSION_PATTERN.fullmatch(rule_version):
            raise RegistryError("detection id and semantic version are required")
        if rule_id in rules:
            raise RegistryError(f"duplicate detection: {rule_id}")
        if algorithm not in SUPPORTED_ALGORITHMS:
            raise RegistryError(f"unsupported detection algorithm: {algorithm}")
        if feature_id not in features:
            raise RegistryError(f"unknown feature for {rule_id}: {feature_id}")
        if severity not in {"low", "medium", "high", "critical"}:
            raise RegistryError(f"invalid severity for {rule_id}")
        generation = _valid_generation(rule.get("generation"), rule_id)
        threshold: int | None = None
        if algorithm in {ALGORITHM_FAILURE_THEN_SUCCESS, ALGORITHM_FAILURE_BURST}:
            threshold = _positive_integer(parameters.get("threshold"), f"{rule_id}.threshold")
        z_threshold: float | None = None
        if algorithm == ALGORITHM_BASELINE_DEVIATION:
            raw_z = parameters.get("z_threshold")
            if not isinstance(raw_z, (int, float)) or isinstance(raw_z, bool) or not math.isfinite(raw_z) or raw_z <= 0:
                raise RegistryError(f"{rule_id}.z_threshold must be a positive finite number")
            z_threshold = float(raw_z)
        rules[rule_id] = RuleDefinition(
            id=rule_id,
            version=rule_version,
            algorithm=algorithm,
            feature_id=feature_id,
            severity=severity,
            threshold=threshold,
            lookback_seconds=_positive_integer(parameters.get("lookback_seconds"), f"{rule_id}.lookback_seconds"),
            window_seconds=_positive_integer(parameters.get("window_seconds"), f"{rule_id}.window_seconds"),
            allowed_lateness_seconds=_positive_integer(
                parameters.get("allowed_lateness_seconds"),
                f"{rule_id}.allowed_lateness_seconds",
            ),
            generation=generation,
            model_id=str(rule.get("model_id", "")) or None,
            z_threshold=z_threshold,
        )

    models: dict[str, ModelDefinition] = {}
    for model in document.get("models", []):
        model_id = str(model.get("id", ""))
        model_version = str(model.get("version", ""))
        if not model_id or not VERSION_PATTERN.fullmatch(model_version):
            raise RegistryError("model id and semantic version are required")
        if model_id in models:
            raise RegistryError(f"duplicate model: {model_id}")
        feature_id = str(model.get("feature_id", ""))
        if feature_id not in features:
            raise RegistryError(f"unknown feature for model {model_id}: {feature_id}")
        feature_version = str(model.get("feature_version", ""))
        if not VERSION_PATTERN.fullmatch(feature_version):
            raise RegistryError(f"model {model_id} feature_version must be semantic version")
        training = model.get("training", {})
        models[model_id] = ModelDefinition(
            id=model_id,
            version=model_version,
            feature_id=feature_id,
            feature_version=feature_version,
            generation=_valid_generation(model.get("generation"), model_id),
            training_interval_seconds=_positive_integer(
                training.get("interval_seconds", DEFAULT_TRAINING_INTERVAL_SECONDS),
                f"{model_id}.training.interval_seconds",
            ),
            min_samples=_positive_integer(training.get("min_samples", 100), f"{model_id}.training.min_samples"),
            min_complete_days=_positive_integer(
                training.get("min_complete_days", 14),
                f"{model_id}.training.min_complete_days",
            ),
        )

    for rule in rules.values():
        if rule.algorithm != ALGORITHM_BASELINE_DEVIATION:
            continue
        if not rule.model_id:
            raise RegistryError(f"baseline deviation rule {rule.id} requires a model_id")
        model = models.get(rule.model_id)
        if model is None:
            raise RegistryError(f"unknown model for {rule.id}: {rule.model_id}")
        if model.feature_id != rule.feature_id:
            raise RegistryError(f"model {model.id} feature does not match rule {rule.id}")

    return AnalysisRegistry(version=version, features=features, rules=rules, models=models)
