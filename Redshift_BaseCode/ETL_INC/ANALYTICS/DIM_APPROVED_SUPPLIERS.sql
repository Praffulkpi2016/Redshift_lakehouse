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

delete from bec_dwh.DIM_APPROVED_SUPPLIERS
where nvl(ASL_ID,0) in (
select nvl(ods.ASL_ID,0) from bec_dwh.DIM_APPROVED_SUPPLIERS dw, bec_ods.po_approved_supplier_list ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.ASL_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_approved_suppliers' and batch_name = 'po')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_APPROVED_SUPPLIERS
(
		  ASL_ID,
		  using_organization_id, 
		  owning_organization_id,
          creation_date,
          last_update_date,
		  created_by,
          vendor_id,
		  item_id,
          item_category_set1,
          item_category_set2,
          vendor_site_id,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
		  ASL_ID,
		  using_organization_id, 
		  owning_organization_id,
          TRUNC (creation_date) creation_date,
          TRUNC (last_update_date) last_update_date,
		  created_by,
          vendor_id,
		  item_id,
          owning_organization_id || '-' || item_id item_category_set1,
          owning_organization_id || '-' || item_id item_category_set2,
          vendor_site_id,
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
    || '-' || nvl(ASL_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
 bec_ods.po_approved_supplier_list 
 where 1=1
 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_approved_suppliers' and batch_name = 'po')
 )
 );

-- Soft delete

update bec_dwh.DIM_APPROVED_SUPPLIERS set is_deleted_flg = 'Y'
where nvl(ASL_ID,0) not in (
select nvl(ods.ASL_ID,0) from bec_dwh.DIM_APPROVED_SUPPLIERS dw, bec_ods.po_approved_supplier_list ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.ASL_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_approved_suppliers' and batch_name = 'po';

COMMIT;