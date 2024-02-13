/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.dim_currency_rates
where (nvl(FROM_CURRENCY,'NA'),nvl(TO_CURRENCY,'NA'),nvl(CONVERSION_DATE,'1900-01-01'),nvl(CONVERSION_TYPE,'NA')) in (
select nvl(ods.FROM_CURRENCY,'NA') as FROM_CURRENCY,
nvl(ods.TO_CURRENCY,'NA') as TO_CURRENCY,
nvl(ods.CONVERSION_DATE,'1900-01-01') as CONVERSION_DATE,
nvl(ods.CONVERSION_TYPE,'NA') as CONVERSION_TYPE
 from bec_dwh.dim_currency_rates dw, bec_ods.GL_DAILY_RATES ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')
||'-'|| nvl(ods.FROM_CURRENCY, 'NA')
||'-'|| nvl(ods.TO_CURRENCY, 'NA')
||'-'|| nvl(ods.CONVERSION_DATE, '1900-01-01')
||'-'|| nvl(ods.CONVERSION_TYPE, 'NA') 
and (ods.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_currency_rates' and batch_name = 'ap') )
);

commit;

-- Insert records

insert into bec_dwh.DIM_CURRENCY_RATES
(
SELECT FROM_CURRENCY,
  TO_CURRENCY,
  CONVERSION_DATE,
  CONVERSION_TYPE,
  RATE_SOURCE_CODE,
  ROUND(CONVERSION_RATE, 5) CONVERSION_RATE,
  ROUND(1/(CONVERSION_RATE), 5) INVERSE_CONVERSION_RATE,
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
       || nvl(FROM_CURRENCY, 'NA')|| '-' || nvl(TO_CURRENCY, 'NA')|| '-' || nvl(CONVERSION_DATE, '1900-01-01') || '-' || nvl(CONVERSION_TYPE, 'NA') AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.GL_DAILY_RATES
 where 1=1
 and (kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_currency_rates' and batch_name = 'ap') )
 );
 
-- Soft delete

update bec_dwh.dim_currency_rates set is_deleted_flg = 'Y'
where (nvl(FROM_CURRENCY,'NA'),nvl(TO_CURRENCY,'NA'),nvl(CONVERSION_DATE,'1900-01-01'),nvl(CONVERSION_TYPE,'NA')) not in (
select nvl(ods.FROM_CURRENCY,'NA') as FROM_CURRENCY,
nvl(ods.TO_CURRENCY,'NA') as TO_CURRENCY,
nvl(ods.CONVERSION_DATE,'1900-01-01') as CONVERSION_DATE,
nvl(ods.CONVERSION_TYPE,'NA') as CONVERSION_TYPE
 from bec_dwh.dim_currency_rates dw, bec_ods.GL_DAILY_RATES ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||
nvl(ods.FROM_CURRENCY, 'NA')|| '-' || 
nvl(ods.TO_CURRENCY, 'NA')|| '-' || 
nvl(ods.CONVERSION_DATE, '1900-01-01') || '-' || 
nvl(ods.CONVERSION_TYPE, 'NA') 
AND ods.is_deleted_flg <> 'Y');

commit; 

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_currency_rates' and batch_name ='ap';

COMMIT;