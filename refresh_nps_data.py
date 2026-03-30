#!/usr/bin/env python3
"""
refresh_nps_data.py
Executa queries de NPS no BigQuery e gera data.js com os dados atualizados.
Coloque este script na mesma pasta que index.html e execute:
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

# ── CTE BASE (reutilizada em todas as queries) ─────────────────────────────────
BASE_CTE = """
WITH BASE_NPS_Y20_DETAIL AS (
  SELECT
    NP.USER_TEAM_ID,
    NP.USER_TEAM_NAME,
    IF(NP.USER_TEAM_ID = 2352, 'C2C', NP.USER_TEAM_CHANNEL) AS USER_TEAM_CHANNEL,
    NP.USER_OFFICE,
    NP.PRO_PROCESS_NAME,
    NP.CDU,
    NP.CX_SOL_NAME,
    NP.ANTIGUEDAD_REP,
    NP.CX_USER_LDAP,
    NP.CX_USER_NAME,
    NP.CX_USER_TEAM_LEADER_LDAP,
    NP.CX_USER_TEAM_LEADER_NAME,
    NP.RES_END_DATE,
    NP.NPS,
    NP.SURVEY_TARGET_VALUE,
    NP.NPS - NP.SURVEY_TARGET_VALUE                          AS GAP_TGT,
    NP.PROMOTER,
    NP.DETRACTOR,
    NP.COMMENTS
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

# ── QUERY 1: agrupamento por office ───────────────────────────────────────────
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

# ── QUERY 2: agrupamento por PRO_PROCESS_NAME (CDU) ───────────────────────────
QUERY_PROCESS = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))   AS PERIODO,
  'MES'                                                           AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET,
  SUM(NP.DETRACTOR)                                              AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6, 7

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
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
GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 3: abertura por senioridade (EXPERT vs NEWBIE) ──────────────────────
QUERY_SENIORITY = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.ANTIGUEDAD_REP                                              AS SENIORITY,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))   AS PERIODO,
  'MES'                                                           AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.ANTIGUEDAD_REP,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK)) AS PERIODO,
  'SEMANA'                                                        AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 4: nível TL (agrupado por CX_USER_TEAM_LEADER_LDAP + processo) ──────
QUERY_TEAM = BASE_CTE + """
SELECT
  NP.CX_USER_TEAM_LEADER_LDAP                                   AS TL_LDAP,
  NP.CX_USER_TEAM_LEADER_NAME                                   AS TL_NAME,
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK)) AS PERIODO,
  'SEMANA'                                                        AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

ORDER BY 5, 4, 3, GAP_TGT
"""

# ── QUERY 5: nível REP individual (CX_USER_LDAP + processo) ───────────────────

# ── QUERY 6: agrupamento por CDU ──────────────────────────────────────────────
QUERY_CDU = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME                                           AS PROCESSO,
  NP.CDU                                                        AS CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))  AS PERIODO,
  'MES'                                                          AS TIPO,
  COUNT(*)                                                       AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                              AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                               AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                  AS TARGET,
  SUM(NP.DETRACTOR)                                             AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6, 7

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
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
GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 6b: cruzamento PROCESSO × CDU (para drill-down no relatório) ────────
QUERY_PROC_CDU = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME                                           AS PROCESSO,
  NP.CDU                                                        AS CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))  AS PERIODO,
  'MES'                                                          AS TIPO,
  COUNT(*)                                                       AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                              AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                               AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                  AS TARGET,
  SUM(NP.DETRACTOR)                                             AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
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
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 7: agrupamento por CX_SOL_NAME ─────────────────────────────────────
QUERY_SOLUCAO = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME                                           AS PROCESSO,
  NP.CX_SOL_NAME                                               AS SOLUCAO,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))  AS PERIODO,
  'MES'                                                          AS TIPO,
  COUNT(*)                                                       AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                              AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                               AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                  AS TARGET,
  SUM(NP.DETRACTOR)                                             AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME,
  NP.CX_SOL_NAME,
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
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1, 2, 3, 4
"""

# ── QUERY DRIVER: DM_CX_NPS_CS_GOALS_MGR_AND_UP — todos os Buyers ─────────────
# Usa exatamente a base indicada pelo usuário: CENTER='BR', sem filtro de SR_MANAGER
# FLAG_QUARTER_MONTH distingue granularidade: MONTH → mensal, WEEK → semanal
QUERY_DRIVER = """
SELECT
  i.DRIVER_TARGET_NPS                                               AS DRIVER,
  a.NPS_TARGET_DRIVER_GROUP,
  i.CX_TEAM_NAME,
  i.CX_USER_TEAM_CHANNEL,
  i.CX_USER_OFFICE,
  i.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', i.DATE_ID)                               AS PERIODO,
  'MES'                                                              AS TIPO,
  CAST(SUM(i.SURVEYS) AS INT64)                                     AS ENCUESTAS,
  ROUND((SUM(i.PROMOTERS) - SUM(i.DETRACTORS)) * 100.0
        / NULLIF(SUM(i.SURVEYS), 0), 2)                            AS NPS,
  ROUND(AVG(i.TARGET_NPS) * 100, 2)                                AS TARGET,
  CAST(SUM(i.DETRACTORS) AS INT64)                                  AS DETRATORES
FROM `meli-bi-data.WHOWNER.DM_CX_NPS_CS_GOALS_MGR_AND_UP` i
LEFT JOIN (
  SELECT NPS_TARGET_DRIVER, ANY_VALUE(NPS_TARGET_DRIVER_GROUP) AS NPS_TARGET_DRIVER_GROUP
  FROM `meli-bi-data.WHOWNER.LK_CX_NPS_CS_GOALS_DRIVER_MANAGER`
  GROUP BY 1
) a ON i.DRIVER_TARGET_NPS = a.NPS_TARGET_DRIVER
WHERE i.DATE_ID >= '2026-01-01'
  AND i.CENTER = 'BR'
  AND i.FLAG_QUARTER_MONTH = 'MONTH'
  AND i.SURVEYS > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

SELECT
  i.DRIVER_TARGET_NPS                                               AS DRIVER,
  a.NPS_TARGET_DRIVER_GROUP,
  i.CX_TEAM_NAME,
  i.CX_USER_TEAM_CHANNEL,
  i.CX_USER_OFFICE,
  i.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', i.DATE_ID)                               AS PERIODO,
  'SEMANA'                                                           AS TIPO,
  CAST(SUM(i.SURVEYS) AS INT64)                                     AS ENCUESTAS,
  ROUND((SUM(i.PROMOTERS) - SUM(i.DETRACTORS)) * 100.0
        / NULLIF(SUM(i.SURVEYS), 0), 2)                            AS NPS,
  ROUND(AVG(i.TARGET_NPS) * 100, 2)                                AS TARGET,
  CAST(SUM(i.DETRACTORS) AS INT64)                                  AS DETRATORES
FROM `meli-bi-data.WHOWNER.DM_CX_NPS_CS_GOALS_MGR_AND_UP` i
LEFT JOIN (
  SELECT NPS_TARGET_DRIVER, ANY_VALUE(NPS_TARGET_DRIVER_GROUP) AS NPS_TARGET_DRIVER_GROUP
  FROM `meli-bi-data.WHOWNER.LK_CX_NPS_CS_GOALS_DRIVER_MANAGER`
  GROUP BY 1
) a ON i.DRIVER_TARGET_NPS = a.NPS_TARGET_DRIVER
WHERE i.DATE_ID >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
  AND i.CENTER = 'BR'
  AND i.FLAG_QUARTER_MONTH = 'WEEK'
  AND i.SURVEYS > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 8: comentários NPS Lineal (verbatins dos últimos 14 dias) ────────────
# ⚠️ ATENÇÃO: verifique o nome correto da coluna de verbatim no seu ambiente BQ.
#    Candidatos comuns: VERBATIM, NPS_VERBATIM, PRO_VERBATIM, VERBATIM_TEXT
#    Substitua 'NP.VERBATIM' abaixo pelo nome real da coluna antes de executar.
QUERY_COMMENTS = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME                                              AS TEAM,
  NP.USER_TEAM_CHANNEL                                          AS CH,
  NP.USER_OFFICE                                                AS OFFICE,
  NP.PRO_PROCESS_NAME                                           AS PROCESSO,
  NP.CDU                                                        AS CDU,
  NP.CX_SOL_NAME                                               AS SOLUCAO,
  NP.ANTIGUEDAD_REP                                             AS SENIORITY,
  FORMAT_DATE('%Y-%m-%d', NP.RES_END_DATE)                     AS DATA,
  CASE
    WHEN NP.NPS < 0.7  THEN 'DETRATOR'
    WHEN NP.NPS >= 0.9 THEN 'PROMOTOR'
    ELSE 'NEUTRO'
  END                                                            AS TIPO,
  NP.COMMENTS                                                   AS COMENTARIO
FROM BASE_NPS_Y20_DETAIL NP
WHERE NP.RES_END_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  AND NP.COMMENTS IS NOT NULL
  AND NP.COMMENTS != ''
ORDER BY NP.RES_END_DATE DESC
LIMIT 500
"""

QUERY_REP = BASE_CTE + """
SELECT
  NP.CX_USER_LDAP                                               AS REP_LDAP,
  NP.CX_USER_NAME                                               AS REP_NAME,
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.ANTIGUEDAD_REP                                             AS SENIORITY,
  NP.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK)) AS PERIODO,
  'SEMANA'                                                        AS TIPO,
  COUNT(*)                                                        AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                               AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                   AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
HAVING COUNT(*) >= 3
ORDER BY 5, 4, 3, GAP_TGT
"""

# ── EXECUÇÃO ──────────────────────────────────────────────────────────────────
def run_query(client, label, sql):
    print(f"[{datetime.now():%H:%M:%S}] {label}...")
    rows = list(client.query(sql).result())
    print(f"[{datetime.now():%H:%M:%S}]   {len(rows)} linhas.")
    return rows

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
    rows_office = run_query(client, "Query 1/5: por office", QUERY_OFFICE)
    office_data = [
        {
            "team":    r.USER_TEAM_NAME,
            "ch":      r.USER_TEAM_CHANNEL,
            "office":  r.USER_OFFICE,
            "period":  str(r.PERIODO),
            "tipo":    r.TIPO,
            "enc":     int(r.ENCUESTAS),
            "gap_tgt": float(r.GAP_TGT) if r.GAP_TGT is not None else None,
            "nps":     float(r.NPS)      if r.NPS      is not None else None,
            "target":  float(r.TARGET)   if r.TARGET   is not None else None,
        }
        for r in rows_office
    ]

    # Query 2 — por processo
    rows_proc = run_query(client, "Query 2/5: por processo (CDU)", QUERY_PROCESS)
    process_data = [
        {
            "team":     r.USER_TEAM_NAME,
            "ch":       r.USER_TEAM_CHANNEL,
            "office":   r.USER_OFFICE,
            "processo": r.PRO_PROCESS_NAME,
            "cdu":      r.CDU if r.CDU is not None else "",
            "period":   str(r.PERIODO),
            "tipo":     r.TIPO,
            "enc":      int(r.ENCUESTAS),
            "gap_tgt":  float(r.GAP_TGT)    if r.GAP_TGT    is not None else None,
            "nps":      float(r.NPS)         if r.NPS        is not None else None,
            "target":   float(r.TARGET)      if r.TARGET     is not None else None,
            "det":      int(r.DETRATORES)    if r.DETRATORES is not None else 0,
        }
        for r in rows_proc
    ]

    # Query 3 — senioridade
    rows_sen = run_query(client, "Query 3/5: senioridade (EXPERT vs NEWBIE)", QUERY_SENIORITY)
    seniority_data = [
        {
            "team":      r.USER_TEAM_NAME,
            "ch":        r.USER_TEAM_CHANNEL,
            "office":    r.USER_OFFICE,
            "seniority": r.SENIORITY,
            "period":    str(r.PERIODO),
            "tipo":      r.TIPO,
            "enc":       int(r.ENCUESTAS),
            "gap_tgt":   float(r.GAP_TGT) if r.GAP_TGT is not None else None,
            "nps":       float(r.NPS)      if r.NPS      is not None else None,
            "target":    float(r.TARGET)   if r.TARGET   is not None else None,
        }
        for r in rows_sen
    ]

    # Query 4 — TL (team_id level)
    rows_team = run_query(client, "Query 4/5: TL level (USER_TEAM_ID)", QUERY_TEAM)
    team_data = [
        {
            "tl_ldap":  r.TL_LDAP,
            "tl_name":  r.TL_NAME,
            "team":     r.USER_TEAM_NAME,
            "ch":       r.USER_TEAM_CHANNEL,
            "office":   r.USER_OFFICE,
            "processo": r.PRO_PROCESS_NAME,
            "period":   str(r.PERIODO),
            "tipo":    r.TIPO,
            "enc":     int(r.ENCUESTAS),
            "gap_tgt": float(r.GAP_TGT) if r.GAP_TGT is not None else None,
            "nps":     float(r.NPS)      if r.NPS      is not None else None,
            "target":  float(r.TARGET)   if r.TARGET   is not None else None,
        }
        for r in rows_team
    ]

    # Query 5 — REP individual
    rep_data = []
    try:
        rows_rep = run_query(client, "Query 5/6: REP individual (CX_USER_LDAP)", QUERY_REP)
        rep_data = [
            {
                "rep_ldap":  r.REP_LDAP,
                "rep_name":  r.REP_NAME,
                "team":      r.USER_TEAM_NAME,
                "ch":        r.USER_TEAM_CHANNEL,
                "office":    r.USER_OFFICE,
                "seniority": r.SENIORITY,
                "processo":  r.PRO_PROCESS_NAME,
                "period":    str(r.PERIODO),
                "tipo":     r.TIPO,
                "enc":      int(r.ENCUESTAS),
                "gap_tgt":  float(r.GAP_TGT) if r.GAP_TGT is not None else None,
                "nps":      float(r.NPS)      if r.NPS      is not None else None,
                "target":   float(r.TARGET)   if r.TARGET   is not None else None,
            }
            for r in rows_rep
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query REP falhou: {e}")
        rep_data = []

    # Query 6 — por CDU (campo direto PRO_CDU_NAME — ajuste o nome se necessário)
    cdu_data = []
    try:
        rows_cdu = run_query(client, "Query 6/8: por CDU", QUERY_CDU)
        cdu_data = [
            {
                "team":    r.USER_TEAM_NAME,
                "ch":      r.USER_TEAM_CHANNEL,
                "office":  r.USER_OFFICE,
                "processo": r.PROCESSO if r.PROCESSO is not None else "",
                "cdu":     r.CDU,
                "period":  str(r.PERIODO),
                "tipo":    r.TIPO,
                "enc":     int(r.ENCUESTAS),
                "gap_tgt": float(r.GAP_TGT)    if r.GAP_TGT    is not None else None,
                "nps":     float(r.NPS)         if r.NPS        is not None else None,
                "target":  float(r.TARGET)      if r.TARGET     is not None else None,
                "det":     int(r.DETRATORES)    if r.DETRATORES is not None else 0,
            }
            for r in rows_cdu
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query CDU falhou: {e}")
        print(f"                            Verifique o campo CDU em QUERY_CDU.")
        cdu_data = []

    # Query 6b — cruzamento PROCESSO × CDU
    proc_cdu_data = []
    try:
        rows_proc_cdu = run_query(client, "Query 6b: PROCESSO × CDU", QUERY_PROC_CDU)
        proc_cdu_data = [
            {
                "team":     r.USER_TEAM_NAME,
                "ch":       r.USER_TEAM_CHANNEL,
                "processo": r.PROCESSO,
                "cdu":      r.CDU,
                "period":   str(r.PERIODO),
                "tipo":     r.TIPO,
                "enc":      int(r.ENCUESTAS),
                "gap_tgt":  float(r.GAP_TGT)    if r.GAP_TGT    is not None else None,
                "nps":      float(r.NPS)         if r.NPS        is not None else None,
                "target":   float(r.TARGET)      if r.TARGET     is not None else None,
                "det":      int(r.DETRATORES)    if r.DETRATORES is not None else 0,
            }
            for r in rows_proc_cdu
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query PROCESSO×CDU falhou: {e}")
        proc_cdu_data = []

    # Query 7 — por SOLUTION_NAME
    solucao_data = []
    try:
        rows_sol = run_query(client, "Query 7/8: por CX_SOL_NAME", QUERY_SOLUCAO)
        solucao_data = [
            {
                "team":    r.USER_TEAM_NAME,
                "ch":      r.USER_TEAM_CHANNEL,
                "processo": r.PROCESSO if r.PROCESSO is not None else "",
                "solucao": r.SOLUCAO,
                "period":  str(r.PERIODO),
                "tipo":    r.TIPO,
                "enc":     int(r.ENCUESTAS),
                "gap_tgt": float(r.GAP_TGT)    if r.GAP_TGT    is not None else None,
                "nps":     float(r.NPS)         if r.NPS        is not None else None,
                "target":  float(r.TARGET)      if r.TARGET     is not None else None,
                "det":     int(r.DETRATORES)    if r.DETRATORES is not None else 0,
            }
            for r in rows_sol
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query Solução falhou: {e}")
        solucao_data = []

    # Query Driver — DM_CX_NPS_CS_GOALS_MGR_AND_UP (todos os Buyers, CENTER=BR)
    driver_data = []
    try:
        rows_driver = run_query(client, "Query Driver: por DRIVER_TARGET_NPS", QUERY_DRIVER)
        driver_data = [
            {
                "driver":   r.DRIVER,
                "group":    r.NPS_TARGET_DRIVER_GROUP,
                "team":     r.CX_TEAM_NAME,
                "ch":       r.CX_USER_TEAM_CHANNEL,
                "office":   r.CX_USER_OFFICE,
                "processo": r.PRO_PROCESS_NAME,
                "period":   str(r.PERIODO),
                "tipo":     r.TIPO,
                "enc":      int(r.ENCUESTAS),
                "nps":      float(r.NPS)         if r.NPS        is not None else None,
                "target":   float(r.TARGET)      if r.TARGET     is not None else None,
                "det":      int(r.DETRATORES)    if r.DETRATORES is not None else 0,
            }
            for r in rows_driver
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query Driver falhou: {e}")
        print(f"                            Verifique QUERY_DRIVER e a tabela DM_CX_NPS_CS_GOALS_MGR_AND_UP.")
        driver_data = []

    # Query 8 — comentários NPS Lineal
    # ⚠️ Se falhar, ajuste o nome da coluna VERBATIM em QUERY_COMMENTS
    comments_data = []
    try:
        rows_comments = run_query(client, "Query 6/6: comentários NPS Lineal", QUERY_COMMENTS)
        comments_data = [
            {
                "team":      r.TEAM,
                "ch":        r.CH,
                "office":    r.OFFICE,
                "processo":  r.PROCESSO,
                "cdu":       r.CDU       if r.CDU       is not None else "",
                "solucao":   r.SOLUCAO   if r.SOLUCAO   is not None else "",
                "seniority": r.SENIORITY,
                "data":      str(r.DATA),
                "tipo":      r.TIPO,
                "comentario": r.COMENTARIO,
            }
            for r in rows_comments
            if r.COMENTARIO
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query Comentários falhou: {e}")
        print(f"                            Verifique o nome da coluna VERBATIM em QUERY_COMMENTS.")
        comments_data = []

    # Gerar data.js
    js_content = (
        f"// Gerado automaticamente por refresh_nps_data.py\n"
        f"// Atualizado em: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"window.NPS_DATA = {json.dumps(office_data,    ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_PROCESS_DATA = {json.dumps(process_data,  ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_SENIORITY_DATA = {json.dumps(seniority_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_TEAM_DATA = {json.dumps(team_data,      ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_REP_DATA = {json.dumps(rep_data,         ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_CDU_DATA = {json.dumps(cdu_data,         ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_PROC_CDU_DATA = {json.dumps(proc_cdu_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_SOLUCAO_DATA = {json.dumps(solucao_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_COMMENTS_DATA = {json.dumps(comments_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_DRIVER_DATA = {json.dumps(driver_data, ensure_ascii=False, indent=2)};\n"
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"[{datetime.now():%H:%M:%S}] Arquivo gerado: {OUTPUT_FILE}")

    # ── publica no GitHub Pages ───────────────────────────────────────────────
    def git(*args):
        result = subprocess.run([GIT, *args], cwd=REPO_DIR, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  git {' '.join(args)} -> ERRO: {result.stderr.strip()}")
        return result.returncode == 0

    print(f"[{datetime.now():%H:%M:%S}] Publicando no GitHub...")
    git("add", "data.js")
    git("commit", "-m", f"dados: atualização automática {datetime.now():%Y-%m-%d %H:%M}")
    ok1 = git("push", "origin", "main")
    ok2 = git("push", "waterfallnps", "main")
    if ok1 or ok2:
        print(f"[{datetime.now():%H:%M:%S}] Publicado! Relatorios atualizados em:")
        print("  https://biancahase-cmyk.github.io/nps_gap_offices/")
        print("  https://biancahase-cmyk.github.io/waterfallnps/waterfall_impactos.html")
    else:
        print(f"[{datetime.now():%H:%M:%S}] Erro no push -- verifique credenciais do Git.")


if __name__ == "__main__":
    main()
