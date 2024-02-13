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

delete from bec_dwh.dim_journal_sources
where (nvl(je_source_name,'NA')) in (
select nvl(ods.je_source_name,'NA') as je_source_name from bec_dwh.dim_journal_sources dw, bec_ods.gl_je_sources_tl ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.je_source_name,'NA')
and ods.language = 'US'
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_journal_sources' and batch_name = 'gl')
 )
);

commit;

-- Insert records

insert into bec_dwh.dim_journal_sources
(
je_source_name,
description,
is_deleted_flg,
source_app_id,
dw_load_id,
dw_insert_date,
dw_update_date
)
(
select 
	je_source_name as je_source_name,
	description as description,
	-- audit columns
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||
		nvl(je_source_name,'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from bec_ods.gl_je_sources_tl  
where 1=1
and language = 'US'
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_journal_sources' and batch_name = 'gl')
 )
);

-- Soft delete

update bec_dwh.dim_journal_sources set is_deleted_flg = 'Y'
where (nvl(je_source_name,'NA')) not in (
select nvl(ods.je_source_name,'NA') as je_source_name from bec_dwh.dim_journal_sources dw, bec_ods.gl_je_sources_tl ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.je_source_name,'NA')
and ods.language = 'US'
AND ods.is_deleted_flg <> 'Y'
);

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_journal_sources'
and batch_name = 'gl';

COMMIT;