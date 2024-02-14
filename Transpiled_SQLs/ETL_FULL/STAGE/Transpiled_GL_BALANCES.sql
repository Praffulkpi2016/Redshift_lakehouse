/* Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.# Unless required by applicable law or agreed to in writing, softwaredistributed under the License is distributed on an "AS IS" BASIS, WITHOUTWARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.# author: KPI Partners, Inc.version: 2022.06description: This script represents full load approach for stage.File Version: KPI v1.0
*/
DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_BALANCES;
CREATE TABLE bronze_bec_ods_stg.GL_BALANCES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.GL_BALANCES
  WHERE
    kca_operation <> 'DELETE'
    AND (LEDGER_ID, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NAME, ACTUAL_FLAG, COALESCE(BUDGET_VERSION_ID, 0), COALESCE(ENCUMBRANCE_TYPE_ID, 0), COALESCE(TRANSLATED_FLAG, '0'), last_update_date) IN (
      SELECT
        LEDGER_ID,
        CODE_COMBINATION_ID,
        CURRENCY_CODE,
        PERIOD_NAME,
        ACTUAL_FLAG,
        COALESCE(BUDGET_VERSION_ID, 0),
        COALESCE(ENCUMBRANCE_TYPE_ID, 0),
        COALESCE(TRANSLATED_FLAG, '0'),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.GL_BALANCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        LEDGER_ID,
        CODE_COMBINATION_ID,
        CURRENCY_CODE,
        PERIOD_NAME,
        ACTUAL_FLAG,
        COALESCE(BUDGET_VERSION_ID, 0),
        COALESCE(ENCUMBRANCE_TYPE_ID, 0),
        COALESCE(TRANSLATED_FLAG, '0')
    )
);