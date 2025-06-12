# HouseHoldAccount

このリポジトリは家計簿の記録をCSVファイルから読み込み、
月次の集計や繰越データの生成を行うPythonスクリプト群です。
`processor` ディレクトリにある `CSVProcessor` クラスを用いて、
入力データの整形・重複排除・カテゴリ別集計・繰越データ生成までを自動化します。

## 主な機能

- 日付・摘要・金額から一意なIDを自動生成
- `codes.csv` に基づく勘定科目・カテゴリの付与
- 各月の入出金をカテゴリ別・科目別に集計
- 月末時点の残高を繰越データとして翌月へ反映
- 処理結果(`results_financial.csv`)とサマリー(`summary.csv`)を出力

## 使い方

1. **環境準備**  
   Python 3.11 以上と以下のパッケージをインストールしてください。
   ```bash
   pip install pandas numpy python-dateutil pytz
   ```
2. **入力ファイルの用意**  
   `datas/forinput.csv` に以下の形式でデータを用意します。
   ```csv
   Date,SubjectCode,Amount,Remarks
   20240303,200,-2000,Starbucks 001
   ...
   ```
3. **スクリプトの実行**  
   リポジトリのルートで次のコマンドを実行します。
   ```bash
   python main.py
   ```
   実行が完了すると、`datas/results_financial.csv` と
   `datas/summary.csv` が生成されます。

## ファイル構成

- `main.py` : 処理のエントリーポイント。
- `processor/processor.py` : CSV処理のロジックをまとめたクラス。
- `codes.csv` : 勘定科目コードとカテゴリ定義。
- `tests/` : 処理結果を検証するためのテストとサンプルデータ。

## テスト

`pytest` を実行することでユニットテストを実行できます。
テストには `pandas` などの依存パッケージが必要です。

```bash
pytest -q
```

## ライセンス

MIT License
