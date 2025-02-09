# これは何？
Talk to the City の改良してみるリポジトリ

- [改良のアイディア](https://github.com/shingo-ohki/talk-to-the-city-reports/issues)
- [改良を試みた（ている）もの](https://github.com/shingo-ohki/talk-to-the-city-reports/pulls)

試験的にこのリポジトリの develop ブランチを https://t3c.dyndns.org にデプロイ中

この辺りでコミュニケーションが行われている
- [Code for Japan のブロードリスニングのプロジェクトページ](https://code4japan-community.notion.site/b018bcee86cb4e03829cade5cb62cee6)
  - 上記に案内のある Code for Japan の Slack の #proj-broadlistening チャンネル
- [安野貴博さんのチームのリポジトリ](https://github.com/takahiroanno2024/anno-broadlistening)

# Talk to the City

This repo is now a merged monorepo containing:

## Talk to the City Turbo

This is the [graph-based reports application](./turbo). It is JS / TS based, it generates interactive reports, and a very wide variety of LLM apps.

e.g

[Heal Michigan](https://tttc-turbo.web.app/report/heal-michigan-9)  
[Taiwan same-sex marriage](https://tttc-turbo.web.app/report/taiwan-zh)  
[Mina protocol](https://tttc-turbo.web.app/report/mina-protocol)


## Talk to the City Reports

This is the [CLI based reports application](./scatter). It is python and next based, and generates static interactive scatter-plot reports with summaries.

e.g

[Heal Michigan](https://tttc.dev/heal-michigan)  
[Recursive Public](https://tttc.dev/recursive)  
[GenAI Taiwan](https://tttc.dev/genai)
