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

delete from bec_dwh.DIM_AP_PAYMENT_METHODS
where (nvl(PAYMENT_METHOD_CODE, 'NA')) in (
select nvl(ods.PAYMENT_METHOD_CODE, 'NA') as PAYMENT_METHOD_CODE from bec_dwh.dim_ap_payment_methods dw,
(SELECT PAYMENT_METHOD_CODE 
 FROM bec_ods.iby_payment_methods_tl 
 WHERE Language = 'US'
 and kca_seq_date > (select (executebegints-prune_days) 
 from bec_etl_ctrl.batch_dw_info where 
 dw_table_name ='dim_ap_payment_methods' and batch_name = 'ap')) ods
where dw.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')|| '-'|| nvl(ods.PAYMENT_METHOD_CODE, 'NA')
);

commit;

-- Insert records

insert into bec_dwh.dim_ap_payment_methods
(
 PAYMENT_METHOD_CODE,
 PAYMENT_METHOD_NAME ,
 DESCRIPTION,
is_deleted_flg,
SOURCE_APP_ID,
DW_LOAD_ID,
DW_INSERT_DATE,
DW_UPDATE_DATE
)
(
SELECT  PAYMENT_METHOD_CODE,
 PAYMENT_METHOD_NAME ,
 DESCRIPTION,
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
       || nvl(PAYMENT_METHOD_CODE, 'NA') AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.iby_payment_methods_tl 
 where language = 'US'
 and (
 kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_payment_methods' and batch_name = 'ap')
 ) 
 );
 commit;
 -- Soft delete

update bec_dwh.dim_ap_payment_methods set is_deleted_flg = 'Y'
where (nvl(PAYMENT_METHOD_CODE, 'NA')) not in (
select nvl(ods.PAYMENT_METHOD_CODE, 'NA') as PAYMENT_METHOD_CODE 
from bec_dwh.dim_ap_payment_methods dw, (select * from bec_ods.iby_payment_methods_tl where language = 'US') ods
where dw.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')|| '-'|| nvl(ods.PAYMENT_METHOD_CODE, 'NA')
AND ods.is_deleted_flg <> 'Y'
);

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_payment_methods'
and batch_name = 'ap';

COMMIT;