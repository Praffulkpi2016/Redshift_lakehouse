/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
drop table if exists bec_dwh.DIM_LEDGERS;

CREATE TABLE bec_dwh.DIM_LEDGERS 
diststyle all 
sortkey(LEDGER_ID)
AS
( SELECT LEDGER_ID,
  NAME "LEDGER_NAME",
  SHORT_NAME "LEDGER_SHORT_NAME",
  DESCRIPTION "LEDGER_DESCRIPTION",
  LEDGER_CATEGORY_CODE,
  CHART_OF_ACCOUNTS_ID,
  CURRENCY_CODE,
  ACCOUNTED_PERIOD_TYPE,
  RET_EARN_CODE_COMBINATION_ID,
  DAILY_TRANSLATION_RATE_TYPE,
  BAL_SEG_COLUMN_NAME,
  BAL_SEG_VALUE_SET_ID,
  SLA_DESCRIPTION_LANGUAGE,
  PERIOD_SET_NAME,
  'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(LEDGER_ID, 0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.GL_LEDGERS
);

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ledgers'  and batch_name = 'ap';

COMMIT;

