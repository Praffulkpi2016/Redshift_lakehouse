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
drop table if exists bec_dwh.DIM_CURRENCY_RATES;

CREATE TABLE bec_dwh.DIM_CURRENCY_RATES 
diststyle all 
sortkey(FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE)
AS
( SELECT FROM_CURRENCY,
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
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_currency_rates' and batch_name = 'ap';

COMMIT;