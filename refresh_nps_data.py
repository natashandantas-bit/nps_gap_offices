#!/usr/bin/env python3
"""
refresh_nps_data.py
Executa queries de NPS no BigQuery e gera data.js com os dados atualizados.
Equipes: BR_ME_Sellers_Longtail, BR_Ventas_Sellers_Longtail, BR_Publicaciones_Sellers_Longtail
Escritórios: AEC, ATE, CTX, KTA_BRASIL
"""

import json
import os
from datetime import datetime

PROJECT_ID  = "meli-bi-data"
REPO_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(REPO_DIR, "data.js")

# ── CTE BASE ──────────────────────────────────────────────────────────────────
BASE_CTE = """
WITH BASE_NPS_Y20_DETAIL AS (
  SELECT
    NP.USER_TEAM_ID,
    NP.USER_TEAM_NAME,
    NP.USER_TEAM_CHANNEL,
    NP.USER_OFFICE,
    NP.PRO_PROCESS_NAME,
    NP.CDU,
    NP.CX_SOL_NAME,
    NP.ANTIGUEDAD_REP,
    NP.CX_USER_LDAP,
    NP.CX_USER_NAME,
    NP.USER_TEAM_LEADER_LDAP  AS CX_USER_TEAM_LEADER_LDAP,
    NP.RES_END_DATE,
    NP.NPS,
    NP.SURVEY_TARGET_VALUE,
    NP.NPS - NP.SURVEY_TARGET_VALUE AS GAP_TGT,
    NP.PROMOTER,
    NP.DETRACTOR,
    NP.COMMENTS,
    NP.SURVEY_CHANNEL
  FROM `meli-bi-data.WHOWNER.DM_CX_NPS_Y20_DETAIL` NP
  INNER JOIN `meli-bi-data.WHOWNER.LK_CX_PLANNING_GROUP` PG
         ON  PG.CX_TEAM_ID = NP.USER_TEAM_ID
         AND PG.FLAG_PLANNING = 'true'
  WHERE NP.RES_END_DATE >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 5 MONTH), MONTH)
    AND NP.FLAG_NOT_EXCLUDED_SURVEY IS TRUE
    AND NP.FLAG_ACTIVE_TEAM        IS TRUE
    AND NP.ANTIGUEDAD_REP IN ('EXPERT', 'NEWBIE')
    AND NP.USER_OFFICE IN ('AEC', 'ATE', 'CTX', 'KTA_BRASIL')
    AND NP.SIT_SITE_ID = 'MLB'
    AND NP.USER_TEAM_NAME IN (
        'BR_ME_Sellers_Longtail',
        'BR_Ventas_Sellers_Longtail',
        'BR_Publicaciones_Sellers_Longtail'
    )
)
"""

# ── QUERY 1: agrupamento por office ───────────────────────────────────────────
QUERY_OFFICE = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.SURVEY_CHANNEL,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))    AS PERIODO,
  'MES'                                                            AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.SURVEY_CHANNEL,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1, 2, 3, 4
"""

# ── QUERY 2: agrupamento por processo (CDU) ───────────────────────────────────
QUERY_PROCESS = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))    AS PERIODO,
  'MES'                                                            AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET,
  SUM(NP.DETRACTOR)                                               AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6, 7

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET,
  SUM(NP.DETRACTOR)                                               AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 3: senioridade (EXPERT vs NEWBIE) ───────────────────────────────────
QUERY_SENIORITY = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.ANTIGUEDAD_REP                                               AS SENIORITY,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))    AS PERIODO,
  'MES'                                                            AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.ANTIGUEDAD_REP,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 4: nível TL ─────────────────────────────────────────────────────────
QUERY_TEAM = BASE_CTE + """
SELECT
  NP.CX_USER_TEAM_LEADER_LDAP                                    AS TL_LDAP,
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY 4, 3, 2, GAP_TGT
"""

# ── QUERY 5: nível REP individual ─────────────────────────────────────────────
QUERY_REP = BASE_CTE + """
SELECT
  NP.CX_USER_LDAP                                                 AS REP_LDAP,
  NP.CX_USER_NAME                                                 AS REP_NAME,
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.ANTIGUEDAD_REP                                               AS SENIORITY,
  NP.PRO_PROCESS_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
HAVING COUNT(*) >= 3

ORDER BY 4, 3, 2, GAP_TGT
"""

# ── QUERY 6: CDU ──────────────────────────────────────────────────────────────
QUERY_CDU = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME                                             AS PROCESSO,
  NP.CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))    AS PERIODO,
  'MES'                                                            AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET,
  SUM(NP.DETRACTOR)                                               AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6, 7

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.USER_OFFICE,
  NP.PRO_PROCESS_NAME,
  NP.CDU,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET,
  SUM(NP.DETRACTOR)                                               AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY 1, 2, 3, 4, 5
"""

# ── QUERY 7: CX_SOL_NAME ──────────────────────────────────────────────────────
QUERY_SOLUCAO = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME                                             AS PROCESSO,
  NP.CX_SOL_NAME                                                  AS SOLUCAO,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, MONTH))    AS PERIODO,
  'MES'                                                            AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET,
  SUM(NP.DETRACTOR)                                               AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  NP.USER_TEAM_NAME,
  NP.USER_TEAM_CHANNEL,
  NP.PRO_PROCESS_NAME,
  NP.CX_SOL_NAME,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NP.RES_END_DATE, ISOWEEK))  AS PERIODO,
  'SEMANA'                                                         AS TIPO,
  COUNT(*)                                                         AS ENCUESTAS,
  ROUND(AVG(NP.GAP_TGT) * 100, 2)                                AS GAP_TGT,
  ROUND((SUM(NP.PROMOTER) - SUM(NP.DETRACTOR)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                                 AS NPS,
  ROUND(AVG(NP.SURVEY_TARGET_VALUE) * 100, 2)                    AS TARGET,
  SUM(NP.DETRACTOR)                                               AS DETRATORES
FROM BASE_NPS_Y20_DETAIL NP
WHERE DATE_TRUNC(NP.RES_END_DATE, ISOWEEK) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK)
GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY 1, 2, 3, 4
"""

# ── QUERY 8: comentários (últimos 14 dias) ────────────────────────────────────
QUERY_COMMENTS = BASE_CTE + """
SELECT
  NP.USER_TEAM_NAME                                               AS TEAM,
  NP.USER_TEAM_CHANNEL                                            AS CH,
  NP.USER_OFFICE                                                  AS OFFICE,
  NP.PRO_PROCESS_NAME                                             AS PROCESSO,
  NP.CDU,
  NP.CX_SOL_NAME                                                  AS SOLUCAO,
  NP.ANTIGUEDAD_REP                                               AS SENIORITY,
  FORMAT_DATE('%Y-%m-%d', NP.RES_END_DATE)                       AS DATA,
  CASE
    WHEN NP.NPS < 0.7  THEN 'DETRATOR'
    WHEN NP.NPS >= 0.9 THEN 'PROMOTOR'
    ELSE 'NEUTRO'
  END                                                              AS TIPO,
  NP.COMMENTS                                                     AS COMENTARIO
FROM BASE_NPS_Y20_DETAIL NP
WHERE NP.RES_END_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  AND NP.COMMENTS IS NOT NULL
  AND NP.COMMENTS != ''
ORDER BY NP.RES_END_DATE DESC
LIMIT 500
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
    rows_office = run_query(client, "Query 1/6: por office", QUERY_OFFICE)
    office_data = [
        {
            "team":      r.USER_TEAM_NAME,
            "ch":        r.USER_TEAM_CHANNEL,
            "survey_ch": r.SURVEY_CHANNEL if r.SURVEY_CHANNEL is not None else "",
            "office":    r.USER_OFFICE,
            "period":    str(r.PERIODO),
            "tipo":      r.TIPO,
            "enc":       int(r.ENCUESTAS),
            "gap_tgt":   float(r.GAP_TGT) if r.GAP_TGT is not None else None,
            "nps":       float(r.NPS)      if r.NPS      is not None else None,
            "target":    float(r.TARGET)   if r.TARGET   is not None else None,
        }
        for r in rows_office
    ]

    # Query 2 — por processo
    rows_proc = run_query(client, "Query 2/6: por processo (CDU)", QUERY_PROCESS)
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
    rows_sen = run_query(client, "Query 3/6: senioridade", QUERY_SENIORITY)
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

    # Query 4 — TL
    rows_team = run_query(client, "Query 4/6: TL level", QUERY_TEAM)
    team_data = [
        {
            "tl_ldap":  r.TL_LDAP,
            "team":     r.USER_TEAM_NAME,
            "ch":       r.USER_TEAM_CHANNEL,
            "office":   r.USER_OFFICE,
            "processo": r.PRO_PROCESS_NAME,
            "period":   str(r.PERIODO),
            "tipo":     r.TIPO,
            "enc":      int(r.ENCUESTAS),
            "gap_tgt":  float(r.GAP_TGT) if r.GAP_TGT is not None else None,
            "nps":      float(r.NPS)      if r.NPS      is not None else None,
            "target":   float(r.TARGET)   if r.TARGET   is not None else None,
        }
        for r in rows_team
    ]

    # Query 5 — REP individual
    rep_data = []
    try:
        rows_rep = run_query(client, "Query 5/6: REP individual", QUERY_REP)
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
                "tipo":      r.TIPO,
                "enc":       int(r.ENCUESTAS),
                "gap_tgt":   float(r.GAP_TGT) if r.GAP_TGT is not None else None,
                "nps":       float(r.NPS)      if r.NPS      is not None else None,
                "target":    float(r.TARGET)   if r.TARGET   is not None else None,
            }
            for r in rows_rep
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query REP falhou: {e}")

    # Query 6 — CDU
    cdu_data = []
    try:
        rows_cdu = run_query(client, "Query 6/6: CDU", QUERY_CDU)
        cdu_data = [
            {
                "team":     r.USER_TEAM_NAME,
                "ch":       r.USER_TEAM_CHANNEL,
                "office":   r.USER_OFFICE,
                "processo": r.PROCESSO if r.PROCESSO is not None else "",
                "cdu":      r.CDU,
                "period":   str(r.PERIODO),
                "tipo":     r.TIPO,
                "enc":      int(r.ENCUESTAS),
                "gap_tgt":  float(r.GAP_TGT)    if r.GAP_TGT    is not None else None,
                "nps":      float(r.NPS)         if r.NPS        is not None else None,
                "target":   float(r.TARGET)      if r.TARGET     is not None else None,
                "det":      int(r.DETRATORES)    if r.DETRATORES is not None else 0,
            }
            for r in rows_cdu
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query CDU falhou: {e}")

    # Query Solução
    solucao_data = []
    try:
        rows_sol = run_query(client, "Query Solução: CX_SOL_NAME", QUERY_SOLUCAO)
        solucao_data = [
            {
                "team":     r.USER_TEAM_NAME,
                "ch":       r.USER_TEAM_CHANNEL,
                "processo": r.PROCESSO if r.PROCESSO is not None else "",
                "solucao":  r.SOLUCAO,
                "period":   str(r.PERIODO),
                "tipo":     r.TIPO,
                "enc":      int(r.ENCUESTAS),
                "gap_tgt":  float(r.GAP_TGT)    if r.GAP_TGT    is not None else None,
                "nps":      float(r.NPS)         if r.NPS        is not None else None,
                "target":   float(r.TARGET)      if r.TARGET     is not None else None,
                "det":      int(r.DETRATORES)    if r.DETRATORES is not None else 0,
            }
            for r in rows_sol
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query Solução falhou: {e}")

    # Query Comentários
    comments_data = []
    try:
        rows_comments = run_query(client, "Query Comentários (14 dias)", QUERY_COMMENTS)
        comments_data = [
            {
                "team":       r.TEAM,
                "ch":         r.CH,
                "office":     r.OFFICE,
                "processo":   r.PROCESSO,
                "cdu":        r.CDU       if r.CDU       is not None else "",
                "solucao":    r.SOLUCAO   if r.SOLUCAO   is not None else "",
                "seniority":  r.SENIORITY,
                "data":       str(r.DATA),
                "tipo":       r.TIPO,
                "comentario": r.COMENTARIO,
            }
            for r in rows_comments
            if r.COMENTARIO
        ]
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}]   AVISO: Query Comentários falhou: {e}")

    # ── Gera data.js ──────────────────────────────────────────────────────────
    js_content = (
        f"// Gerado automaticamente por refresh_nps_data.py\n"
        f"// Atualizado em: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"window.NPS_DATA = {json.dumps(office_data,    ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_PROCESS_DATA = {json.dumps(process_data,   ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_SENIORITY_DATA = {json.dumps(seniority_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_TEAM_DATA = {json.dumps(team_data,      ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_REP_DATA = {json.dumps(rep_data,        ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_CDU_DATA = {json.dumps(cdu_data,        ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_SOLUCAO_DATA = {json.dumps(solucao_data, ensure_ascii=False, indent=2)};\n\n"
        f"window.NPS_COMMENTS_DATA = {json.dumps(comments_data, ensure_ascii=False, indent=2)};\n"
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"[{datetime.now():%H:%M:%S}] ✓ Arquivo gerado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
