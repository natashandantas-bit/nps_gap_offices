#!/usr/bin/env python3
"""
refresh_nps_data.py
Executa queries de NPS no BigQuery e gera data.js com os dados atualizados.
Coloque este script na mesma pasta que nps_gap_offices.html e execute:
    python refresh_nps_data.py
Pré-requisito:  pip install google-cloud-bigquery
Autenticação:   gcloud auth application-default login
"""

import json
import os
import subprocess
from datetime import datetime

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────────────────
PROJECT_ID  = "meli-bi-data"
REPO_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(REPO_DIR, "data.js")
GIT         = r"C:\Users\bhase\AppData\Local\Programs\Git\bin\git.exe"

# ── CTE BASE (reutilizada nas duas queries) ───────────────────────────────────
BASE_CTE = """
WITH BASE_NPS_Y20_DETAIL AS (
  SELECT
    NP.USER_TEAM_ID,
    NP.USER_TEAM_NAME,
    IF(NP.USER_TEAM_ID = 2352, 'C2C', NP.USER_TEAM_CHANNEL) AS USER_TEAM_CHANNEL,
    NP.USER_OFFICE,
    NP.PRO_PROCESS_NAME,
    NP.RES_END_DATE,
    NP.NPS,
    NP.SURVEY_TARGET_VALUE,
    NP.NPS - NP.SURVEY_TARGET_VALUE                          AS GAP_TGT,
    NP.PROMOTER,
    NP.DETRACTOR
  FROM `meli-bi-data.WHOWNER.DM_CX_NPS_Y20_DETAIL` NP
  INNER JOIN `meli-bi-data.WHOWNER.LK_CX_PLANNING_GROUP` PG
         ON  PG.CX_TEAM_ID = NP.USER_TEAM_ID
         AND PG.FLAG_PLANNING = 'true'
  WHERE NP.RES_END_DATE >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 5 MONTH), MONTH)
    AND NP.FLAG_NOT_EXCLUDED_SURVEY IS TRUE
    AND NP.FLAG_ACTIVE_TEAM        IS TRUE
    AND NP.ANTIGUEDAD_REP IN ('EXPERT', 'NEWBIE')
    AND NP.USER_OFFICE IN ('AEC', 'ATE', 'CTX', 'MELICIDADE', 'KTA_BRASIL')
    AND NP.SIT_SITE_ID = 'MLB'
    AND NP.USER_TEAM_NAME NOT LIKE '%CBT_OFFLINE%'
    AND NP.USER_TEAM_NAME IN (
        'BR_Buyers_Pre_Entrega',
        'BR_Buyers_Post_Entrega',
        'BR_Buyers_Compra'
    )
)
"""

# ── QUERY 1: agrupamento por office (para tabelas e gráficos de office) ───────
QUERY_OFFICE = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))   AS PERIODO,
  'MES'                                                           AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK)) AS PERIODO,
  'SEMANA'                                                        AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5

ORDER BY 1, 2, 3, 4
"""

# ── QUERY 2: agrupamento por PRO_PROCESS_NAME (para tabela CDU) ───────────────
QUERY_PROCESS = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))   AS PERIODO,
  'MES'                                                           AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET,
  SUM(NP.DETRACTOR)                                              AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK)) AS PERIODO,
  'SEMANA'                                                        AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET,
  SUM(NP.DETRACTOR)                                              AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5

ORDER BY 1, 2, 3, 4
"""

# ── EXECUÇÃO ──────────────────────────────────────────────────────────────────
def main():
    try:
        from google.cloud import bigquery
    except ImportError:
        print("Erro: biblioteca google-cloud-bigquery não encontrada.")
        print("Execute:  pip install google-cloud-bigquery")
        return

    print(f"[{datetime.now():%H:%M:%S}] Conectando ao BigQuery (projeto: {PROJECT_ID})...")
    client = bigquery.Client(project=PROJECT_ID)

    # Query 1 — por office
    print(f"[{datetime.now():%H:%M:%S}] Query 1/2: agrupamento por office...")
    rows_office = list(client.query(QUERY_OFFICE).result())
    print(f"[{datetime.now():%H:%M:%S}]   {len(rows_office)} linhas.")

    office_data = []
    for row in rows_office:
        office_data.append({
            "team":    row.USER_TEAM_NAME,
            "ch":      row.USER_TEAM_CHANNEL,
            "office":  row.USER_OFFICE,
            "period":  str(row.PERIODO),
            "tipo":    row.TIPO,
            "enc":     int(row.ENCUESTAS),
            "gap_tgt": float(row.GAP_TGT) if row.GAP_TGT is not None else None,
            "nps":     float(row.NPS)      if row.NPS      is not None else None,
            "target":  float(row.TARGET)   if row.TARGET   is not None else None,
        })

    # Query 2 — por processo
    print(f"[{datetime.now():%H:%M:%S}] Query 2/2: agrupamento por processo (PRO_PROCESS_NAME)...")
    rows_proc = list(client.query(QUERY_PROCESS).result())
    print(f"[{datetime.now():%H:%M:%S}]   {len(rows_proc)} linhas.")

    process_data = []
    for row in rows_proc:
        process_data.append({
            "team":     row.USER_TEAM_NAME,
            "ch":       row.USER_TEAM_CHANNEL,
            "processo": row.PRO_PROCESS_NAME,
            "period":   str(row.PERIODO),
            "tipo":     row.TIPO,
            "enc":      int(row.ENCUESTAS),
            "gap_tgt":  float(row.GAP_TGT)    if row.GAP_TGT    is not None else None,
            "nps":      float(row.NPS)         if row.NPS        is not None else None,
            "target":   float(row.TARGET)      if row.TARGET     is not None else None,
            "det":      int(row.DETRATORES)    if row.DETRATORES is not None else 0,
        })

    # Gerar data.js
    js_content = (
        f"// Gerado automaticamente por refresh_nps_data.py\n"
        f"// Atualizado em: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"window.NPS_DATA = {json.dumps(office_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_PROCESS_DATA = {json.dumps(process_data, ensure_ascii=False, indent=2)};\n"
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"[{datetime.now():%H:%M:%S}] Arquivo gerado: {OUTPUT_FILE}")

    # ── publica no GitHub Pages ───────────────────────────────────────────────
    def git(*args):
        result = subprocess.run([GIT, *args], cwd=REPO_DIR, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  git {' '.join(args)} → ERRO: {result.stderr.strip()}")
        return result.returncode == 0

    print(f"[{datetime.now():%H:%M:%S}] Publicando no GitHub...")
    git("add", "data.js")
    git("commit", "-m", f"dados: atualização automática {datetime.now():%Y-%m-%d %H:%M}")
    if git("push", "origin", "main"):
        print(f"[{datetime.now():%H:%M:%S}] Publicado! Relatório atualizado em:")
        print("  https://biancahase-cmyk.github.io/nps_gap_offices/")
    else:
        print(f"[{datetime.now():%H:%M:%S}] Erro no push — verifique credenciais do Git.")


if __name__ == "__main__":
    main()
