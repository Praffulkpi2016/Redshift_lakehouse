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

delete from bec_dwh.DIM_PO_TYPES
where (nvl(PO_TYPE,'0'),nvl(LOOKUP_TYPE,'0')) in (
select nvl(ods.LOOKUP_CODE,'0'),nvl(ods.LOOKUP_TYPE,'0') from bec_dwh.DIM_PO_TYPES dw, 
(select LOOKUP_CODE,LOOKUP_TYPE from bec_ods.PO_LOOKUP_CODES 
where 1=1 and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_types' 
and batch_name = 'po')
 )ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'|| nvl(ods.LOOKUP_CODE,'0')
||'-'|| nvl(ods.LOOKUP_TYPE,'0')
);

commit;

-- Insert records

insert into bec_dwh.DIM_PO_TYPES
(
	PO_TYPE,
	LOOKUP_TYPE, 
	PO_TYPE_DISPLAY,
	PO_TYPE_DESC,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
	LOOKUP_CODE as PO_TYPE,
	LOOKUP_TYPE,
	DISPLAYED_FIELD as PO_TYPE_DISPLAY, 
	DESCRIPTION  as PO_TYPE_DESC,
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    ||'-'|| nvl(LOOKUP_CODE,'0')
    ||'-'|| nvl(LOOKUP_TYPE,'0') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.PO_LOOKUP_CODES
Where	1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_types' 
and batch_name = 'po')
 )
);
commit;
-- Soft delete

update bec_dwh.DIM_PO_TYPES set  is_deleted_flg = 'Y'
where (nvl(PO_TYPE,'0'),nvl(LOOKUP_TYPE,'0')) not in (
select nvl(ods.LOOKUP_CODE,'0'),nvl(ods.LOOKUP_TYPE,'0') from bec_dwh.DIM_PO_TYPES dw, 
(select LOOKUP_CODE,LOOKUP_TYPE from bec_ods.PO_LOOKUP_CODES 
where 1=1 AND is_deleted_flg <>'Y'
and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_types' 
and batch_name = 'po')
 )ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'|| nvl(ods.LOOKUP_CODE,'0')
||'-'|| nvl(ods.LOOKUP_TYPE,'0')
)
;
commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_types' and batch_name = 'po';

COMMIT;