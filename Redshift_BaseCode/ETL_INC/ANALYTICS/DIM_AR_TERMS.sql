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

delete from bec_dwh.DIM_AR_TERMS
where (nvl(TERM_ID,0)) in
(
select nvl(ods.TERM_ID,0) as TERM_ID from bec_dwh.DIM_AR_TERMS dw, 
bec_ods.RA_TERMS_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.TERM_ID,0)
and (ods.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_terms' and batch_name = 'ar') )
);

commit;

-- Insert records

insert into bec_dwh.DIM_AR_TERMS
(
term_id,
	description,
	"name",
	"language",
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
TERM_ID,
DESCRIPTION,
NAME,
"LANGUAGE",
last_update_date,
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
    || '-'
       || nvl(TERM_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.RA_TERMS_TL
WHERE 1=1
and (kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_terms' and batch_name = 'ar')
 )
 );

-- Soft delete

update bec_dwh.DIM_AR_TERMS set is_deleted_flg = 'Y'
where (nvl(TERM_ID,0)) not in (
select nvl(ods.TERM_ID,0) as TERM_ID from bec_dwh.DIM_AR_TERMS dw, 
bec_ods.RA_TERMS_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.TERM_ID,0) 
AND ods.is_deleted_flg <> 'Y'
);

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_terms' and batch_name = 'ar';

COMMIT;