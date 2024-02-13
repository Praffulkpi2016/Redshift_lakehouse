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
drop table if exists bec_dwh.DIM_AP_PAYMENT_METHODS;

CREATE TABLE bec_dwh.dim_ap_payment_methods 
diststyle all 
sortkey(PAYMENT_METHOD_CODE)
AS
(
SELECT 
 PAYMENT_METHOD_CODE,
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
    )           AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(PAYMENT_METHOD_CODE,'NA') 
	 AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
	bec_ods.iby_payment_methods_tl 
WHERE language = 'US'
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_payment_methods' 
	and batch_name = 'ap';

COMMIT;