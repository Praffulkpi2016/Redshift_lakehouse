/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.dim_ledgers
where (nvl(LEDGER_ID, 0)) in (
select nvl(ods.LEDGER_ID, 0) as LEDGER_ID from bec_dwh.dim_ledgers dw, bec_ods.GL_LEDGERS ods
where dw.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')|| '-'|| nvl(ods.LEDGER_ID, 0)
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ledgers' and batch_name = 'ap')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_LEDGERS
(
LEDGER_ID,
LEDGER_NAME,
LEDGER_SHORT_NAME,
LEDGER_DESCRIPTION,
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
is_deleted_flg,
SOURCE_APP_ID,
DW_LOAD_ID,
DW_INSERT_DATE,
DW_UPDATE_DATE
)
(
SELECT LEDGER_ID,
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
 where 1=1
 and (
 kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ledgers' and batch_name = 'ap')
 ) 
 );
 
 -- Soft delete

update bec_dwh.dim_ledgers set is_deleted_flg = 'Y'
where (nvl(LEDGER_ID, 0)) not in (
select nvl(ods.LEDGER_ID, 0) as LEDGER_ID from bec_dwh.dim_ledgers dw, bec_ods.GL_LEDGERS ods
where dw.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')|| '-'|| nvl(ods.LEDGER_ID, 0)
AND ods.is_deleted_flg <> 'Y'
);

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ledgers'
and batch_name = 'ap';

COMMIT;