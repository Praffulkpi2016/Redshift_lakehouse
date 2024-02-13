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
DELETE FROM bec_dwh.DIM_AP_CHECK_FORMATS
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.ap_check_formats ods
    WHERE ods.check_format_id = dim_ap_check_formats.check_format_id
    AND ods.kca_seq_date > (
        SELECT executebegints - prune_days
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_ap_check_formats'
        AND batch_name = 'ap'
    )
);
commit;

-- Insert records

insert into bec_dwh.DIM_AP_CHECK_FORMATS 
(
check_format_id
,check_format
,invoices_per_stub
,payment_method
,currency_code
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
select check_format_id,
       name "CHECK_FORMAT",
       invoices_per_stub,
       payment_method_lookup_code "PAYMENT_METHOD",
       currency_code,
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
       || nvl(check_format_id,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.ap_check_formats
	where 1=1
	and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_check_formats' and batch_name = 'ap')
 )
	);
commit;
-- Soft delete

update bec_dwh.dim_ap_check_formats set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
    SELECT 1
    FROM bec_ods.ap_check_formats ods
    WHERE ods.check_format_id = dim_ap_check_formats.check_format_id
    AND ods.is_deleted_flg <> 'Y'
);
commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_ap_check_formats'
	and batch_name = 'ap';

COMMIT;