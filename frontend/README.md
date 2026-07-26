# ETL Pipeline Builder Frontend

React/Vite と Electron で動作するパイプラインエディターです。

## Prerequisites

- Node.js 22.12以降
- npm
- `http://127.0.0.1:8000` で起動したETL Pipeline Builderバックエンド

Electronアプリはフロントエンドのみを梱包します。バックエンドはexeに同梱されないため、
パイプラインの取得・実行には別途バックエンドを起動してください。

## Development

```bash
npm install
npm start
```

ブラウザだけで確認する場合は `npm run dev` を使用します。

## Windows installer

Windows PowerShellで次を実行します。

```powershell
.\build-win-exe.ps1
```

NSISインストーラーは `dist_electron` に出力されます。Windows向けビルドはWindows上で
実行することを推奨します。Linux/WSLからクロスビルドする場合はWineが必要です。

パッケージ版のAPI通信はElectronメインプロセスを経由するため、`file://` のCORS制限を
受けません。既定のAPI URLは `http://127.0.0.1:8000/api/v1` です。別のURLを使う場合は
アプリ起動前に `ETL_API_BASE_URL` 環境変数を設定してください。

コード署名証明書とアプリアイコンは現在設定されていません。そのため配布時にはWindows
SmartScreenの警告とElectronの既定アイコンが表示されます。

## Checks

```bash
npm run lint
npm run build
```
