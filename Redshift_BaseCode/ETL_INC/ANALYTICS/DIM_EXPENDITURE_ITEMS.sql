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

delete from bec_dwh.DIM_EXPENDITURE_ITEMS
where (nvl(EXPENDITURE_ITEM_ID,0) ) in (
select nvl(ods.EXPENDITURE_ITEM_ID,0)  
from bec_dwh.DIM_EXPENDITURE_ITEMS dw, bec_ods.pa_expenditure_items_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'||nvl(ods.EXPENDITURE_ITEM_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name  ='dim_expenditure_items' and batch_name = 'gl')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_EXPENDITURE_ITEMS
(
 expenditure_item_id,
	expenditure_id,
	expenditure_type,
	orig_transaction_reference,
	transaction_source,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
SELECT 
 expenditure_item_id,
	expenditure_id,
	expenditure_type,
	orig_transaction_reference,
	transaction_source 
,'N' as is_deleted_flg
,(select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
(select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-' || nvl(EXPENDITURE_ITEM_ID, 0) 	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.pa_expenditure_items_all
 where 1=1
 and (kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name  ='dim_expenditure_items' and batch_name = 'gl')
 )
 );

-- Soft delete

update bec_dwh.DIM_EXPENDITURE_ITEMS set is_deleted_flg = 'Y'
where (nvl(EXPENDITURE_ITEM_ID,0)  ) not in (
select nvl(ods.EXPENDITURE_ITEM_ID,0) as EXPENDITURE_ITEM_ID   from bec_dwh.DIM_EXPENDITURE_ITEMS dw, bec_ods.pa_expenditure_items_all ods
where dw.dw_load_id = (select system_id 
from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')
||'-'|| nvl(ods.EXPENDITURE_ITEM_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_expenditure_items' and batch_name = 'gl';

COMMIT;