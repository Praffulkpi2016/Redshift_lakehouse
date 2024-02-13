/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
DROP TABLE IF EXISTS bec_ods_stg.GL_BALANCES; 

CREATE TABLE bec_ods_stg.GL_BALANCES 
DISTKEY (CODE_COMBINATION_ID)
SORTKEY (
  LEDGER_ID,
  CODE_COMBINATION_ID,
  CURRENCY_CODE,
  PERIOD_NAME,
  ACTUAL_FLAG,
  BUDGET_VERSION_ID,
  ENCUMBRANCE_TYPE_ID,
  TRANSLATED_FLAG,
  last_update_date
)
AS (SELECT * FROM bec_raw_dl_ext.GL_BALANCES
WHERE kca_operation != 'DELETE' and 
(LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE, PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID,0),nvl(ENCUMBRANCE_TYPE_ID,0),nvl(TRANSLATED_FLAG,'0'),last_update_date)
in (select LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID,0),nvl(ENCUMBRANCE_TYPE_ID,0),nvl(TRANSLATED_FLAG,'0'),max(last_update_date) from bec_raw_dl_ext.GL_BALANCES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID,0),nvl(ENCUMBRANCE_TYPE_ID,0),nvl(TRANSLATED_FLAG,'0')
)
);

END;
