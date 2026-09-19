# Arsenal Scout Intelligence

アーセナルの試合スタッツから構造的な弱点を検出し、その弱点を補える戦術ロールと市場候補を、検索根拠とKnowledge Graphの経路付きで提示する実働プロトタイプです。

> **重要:** 同梱の試合、選手、クラブ、価格、指標はすべて架空のデモデータです。現在のアーセナルや実在選手の評価ではありません。本番利用では契約済みデータプロバイダーへ差し替えてください。

## まず動かす

Python 3.11 以上だけで動きます。

```bash
cd arsenal-scout
PYTHONPATH=src python -m arsenal_scout --port 8765
```

ブラウザで `http://127.0.0.1:8765` を開きます。

テスト:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

## 実装済みの要素

| 要素 | 実装 |
|---|---|
| ベクトル化 | 全Match/Weakness/Player文書を384次元へ変換。デモは再現可能なローカルHash Embedding |
| RAG | 検索結果、弱点診断、候補ランキングを一つの根拠コンテキストに統合 |
| ハイブリッド検索 | BM25語句一致 48% + ベクトル類似 52%、オントロジーでクエリ拡張、選手探索意図を補正 |
| Knowledge Graph | Team / Match / Metric / Weakness / Role / Player / Club / Source のProperty Graph |
| GraphRAG | `Weakness → Role → Player` と試合根拠をサブグラフ化し、説明経路を返却 |
| オントロジー | エンティティ、関係、弱点、戦術ロール、必要指標をJSONでバージョン管理 |
| セマンティックレイヤー | 指標の粒度、単位、集約、良悪方向、ピア基準、文脈フィルタを一元化 |
| ワークフローグラフ | validate → diagnose → retrieve/rank → graph_expand → explain の再実行可能DAG |
| Observability | trace/span、ノード遅延、エラー、件数、直近スコアをAPIと画面で確認 |
| MCP | 分析、根拠検索、GraphRAG、health の4ツールをstdio JSON-RPCで公開 |

## API

- `GET /api/report?budget_m=80&q=...` — 全分析
- `GET /api/search?q=...&kind=player` — ハイブリッド検索
- `GET /api/graph` — Knowledge Graph全体
- `GET /api/workflow` — ワークフローDAG
- `GET /api/semantic-layer` — 指標定義
- `GET /api/observability` — spans / counters / gauges
- `GET /api/health` — health check

## MCPとして接続

インストール後なら次で起動できます。

```bash
python -m pip install -e .
arsenal-scout-mcp
```

クライアント設定例は `mcp.example.json`。公開ツール:

- `arsenal_analyze_weaknesses`
- `arsenal_search_evidence`
- `arsenal_graphrag_context`
- `arsenal_scout_health`

## 本番データへの差し替え順

1. 契約したイベント/トラッキングデータを `team_match` 粒度へ正規化する。
2. `semantic_metrics.json` のピア基準を同リーグ・同期間・同ポゼッション条件で再計算する。
3. 選手指標をポジション、リーグ強度、年齢、出場時間で補正した百分位へ変換する。
4. Hash Embeddingを多言語Sentence Transformerまたは埋め込みAPIへ差し替える。
5. BM25をOpenSearch、Vectorをpgvector、GraphをNeo4j/Neptuneへ移す。
6. OpenTelemetry Collectorへspanを送り、データ鮮度とランキングドリフトのアラートを追加する。
7. 映像・負傷歴・契約・ホームグロウン・登録枠を**必須の人間承認ゲート**にする。

詳しい構成は [docs/architecture.md](docs/architecture.md)、入力仕様は [docs/data-contracts.md](docs/data-contracts.md) を参照してください。

## ディレクトリ

```text
arsenal-scout/
├── data/                    # ontology, semantic catalog, demo datasets
├── docs/                    # architecture and contracts
├── src/arsenal_scout/
│   ├── semantic_layer.py    # metric definitions → weakness signals
│   ├── hybrid_search.py     # BM25 + vectors + query expansion
│   ├── knowledge_graph.py   # property graph + GraphRAG paths
│   ├── ranking.py           # role-aware candidate scoring
│   ├── workflow.py          # deterministic DAG
│   ├── observability.py     # traces, metrics, latency
│   ├── engine.py            # orchestration facade
│   ├── server.py            # API + dashboard server
│   └── mcp_server.py        # MCP JSON-RPC server
├── web/                     # zero-dependency dashboard
└── tests/
```

## 判断上の制約

- 小標本の差を断定しないため、信頼度とサンプル数を出します。
- 指標の重複相関は今後の階層モデルで補正が必要です。
- 価格は予測値ではなく別モデル/一次情報に分離すべきです。
- 推薦は選手の人格、医療情報、代理人事情を推測しません。
- ランキングは意思決定支援であり、自動獲得判断ではありません。
