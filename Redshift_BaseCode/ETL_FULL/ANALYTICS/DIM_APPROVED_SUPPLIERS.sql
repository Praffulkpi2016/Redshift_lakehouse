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
drop table if exists bec_dwh.DIM_APPROVED_SUPPLIERS;

CREATE TABLE  bec_dwh.DIM_APPROVED_SUPPLIERS 
	diststyle all sortkey(ASL_ID)
as
(
select 
		  a.ASL_ID,
		  a.using_organization_id, 
		  a.owning_organization_id,
          TRUNC (a.creation_date) creation_date,
          TRUNC (a.last_update_date) last_update_date,
		  a.created_by,
          a.vendor_id,
		  a.item_id,
          a.owning_organization_id || '-' || a.item_id item_category_set1,
          a.owning_organization_id || '-' || a.item_id item_category_set2,
          a.vendor_site_id,
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
       || nvl(ASL_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
from BEC_ODS.po_approved_supplier_list a
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_approved_suppliers'
	and batch_name = 'po';

commit;