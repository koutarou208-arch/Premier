from __future__ import annotations

import hashlib
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable


TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+|[\u3040-\u30ff\u3400-\u9fff]+")


def tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for raw in TOKEN_RE.findall(text.lower()):
        tokens.append(raw)
        if any("\u3040" <= char <= "\u9fff" for char in raw) and len(raw) > 2:
            tokens.extend(raw[index : index + 2] for index in range(len(raw) - 1))
    return tokens


@dataclass
class Document:
    id: str
    kind: str
    title: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


class HashEmbedding:
    """Deterministic, offline embedding fallback.

    It is intentionally provider-compatible: replace ``embed`` with an API or
    sentence-transformer implementation for production quality vectors.
    """

    def __init__(self, dimensions: int = 384) -> None:
        self.dimensions = dimensions

    def embed(self, text: str) -> list[float]:
        vector = [0.0] * self.dimensions
        tokens = tokenize(text)
        features = tokens + [f"{a}::{b}" for a, b in zip(tokens, tokens[1:])]
        for feature in features:
            digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "big") % self.dimensions
            sign = 1.0 if digest[4] & 1 else -1.0
            vector[bucket] += sign * (1.0 + min(len(feature), 20) / 40)
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]


def cosine(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


class HybridIndex:
    def __init__(self, documents: Iterable[Document], ontology: dict[str, Any]) -> None:
        self.documents = list(documents)
        self.ontology = ontology
        self.embedding = HashEmbedding()
        self.doc_tokens = {doc.id: tokenize(f"{doc.title} {doc.text}") for doc in self.documents}
        self.doc_vectors = {doc.id: self.embedding.embed(f"{doc.title} {doc.text}") for doc in self.documents}
        self.avg_length = sum(map(len, self.doc_tokens.values())) / max(len(self.documents), 1)
        self.df: Counter[str] = Counter()
        for tokens in self.doc_tokens.values():
            self.df.update(set(tokens))

    def _expand_query(self, query: str) -> str:
        lower = query.lower()
        extras: list[str] = []
        for weakness_id, item in self.ontology["weaknesses"].items():
            haystack = " ".join([weakness_id, item["label_ja"], *item["search_terms"]]).lower()
            if any(token in haystack for token in tokenize(lower)):
                extras.extend(item["search_terms"])
                extras.extend(item["target_roles"])
        return " ".join([query, *extras])

    def _bm25(self, query_tokens: list[str], doc_id: str) -> float:
        frequencies = Counter(self.doc_tokens[doc_id])
        doc_length = len(self.doc_tokens[doc_id])
        total = len(self.documents)
        score = 0.0
        for token in query_tokens:
            if not frequencies[token]:
                continue
            df = self.df[token]
            idf = math.log(1 + (total - df + 0.5) / (df + 0.5))
            tf = frequencies[token]
            denominator = tf + 1.5 * (1 - 0.75 + 0.75 * doc_length / max(self.avg_length, 1))
            score += idf * (tf * 2.5) / denominator
        return score

    def search(
        self,
        query: str,
        *,
        top_k: int = 8,
        kinds: set[str] | None = None,
        lexical_weight: float = 0.48,
        vector_weight: float = 0.52,
    ) -> list[dict[str, Any]]:
        expanded = self._expand_query(query)
        query_tokens = tokenize(expanded)
        query_vector = self.embedding.embed(expanded)
        player_intent_terms = {"player", "candidate", "recruit", "選手", "候補", "補強", *self.ontology["roles"].keys()}
        has_player_intent = any(term in expanded.lower() for term in player_intent_terms)
        rows = []
        for doc in self.documents:
            if kinds and doc.kind not in kinds:
                continue
            rows.append(
                {
                    "document": doc,
                    "lexical_raw": self._bm25(query_tokens, doc.id),
                    "vector_raw": cosine(query_vector, self.doc_vectors[doc.id]),
                }
            )
        lexical_max = max((row["lexical_raw"] for row in rows), default=1.0) or 1.0
        vector_values = [row["vector_raw"] for row in rows]
        vector_min = min(vector_values, default=0.0)
        vector_max = max(vector_values, default=1.0)
        for row in rows:
            lexical = row["lexical_raw"] / lexical_max
            vector = (row["vector_raw"] - vector_min) / max(vector_max - vector_min, 1e-9)
            row["lexical_score"] = lexical
            row["vector_score"] = vector
            row["intent_boost"] = 0.08 if has_player_intent and row["document"].kind == "player" else 0.0
            row["score"] = min(1.0, lexical_weight * lexical + vector_weight * vector + row["intent_boost"])
        rows.sort(key=lambda row: row["score"], reverse=True)
        output = []
        for row in rows[:top_k]:
            doc = row["document"]
            output.append(
                {
                    "id": doc.id,
                    "kind": doc.kind,
                    "title": doc.title,
                    "text": doc.text,
                    "metadata": doc.metadata,
                    "score": round(row["score"], 4),
                    "lexical_score": round(row["lexical_score"], 4),
                    "vector_score": round(row["vector_score"], 4),
                    "intent_boost": round(row["intent_boost"], 4),
                }
            )
        return output


def build_documents(
    match_data: dict[str, Any],
    players_data: dict[str, Any],
    ontology: dict[str, Any],
    metrics: dict[str, Any],
) -> list[Document]:
    docs: list[Document] = []
    for match in match_data["matches"]:
        metric_text = "; ".join(
            f"{metrics['metrics'][key]['label_ja']} {value} {metrics['metrics'][key]['unit']}"
            for key, value in match["metrics"].items()
            if key in metrics["metrics"]
        )
        docs.append(
            Document(
                id=f"match:{match['id']}",
                kind="match",
                title=f"Arsenal vs {match['opponent']} ({match['date']})",
                text=f"opponent shape {match['opponent_shape']}; result {match['result']}; {metric_text}",
                metadata={"match_id": match["id"], "date": match["date"], "opponent": match["opponent"]},
            )
        )
    for weakness_id, item in ontology["weaknesses"].items():
        docs.append(
            Document(
                id=f"weakness:{weakness_id}",
                kind="weakness",
                title=item["label_ja"],
                text=" ".join([item["description"], *item["search_terms"], *item["target_roles"]]),
                metadata={"weakness_id": weakness_id, "target_roles": item["target_roles"]},
            )
        )
    for player in players_data["players"]:
        role_labels = [ontology["roles"][role]["label_ja"] for role in player["roles"]]
        metric_text = " ".join(f"{key} {value}" for key, value in player["metrics"].items())
        docs.append(
            Document(
                id=f"player:{player['id']}",
                kind="player",
                title=player["name"],
                text=f"roles {' '.join(player['roles'])} {' '.join(role_labels)}; side {player['preferred_side']}; {metric_text}",
                metadata={"player_id": player["id"], "roles": player["roles"], "club": player["club"]},
            )
        )
    return docs
