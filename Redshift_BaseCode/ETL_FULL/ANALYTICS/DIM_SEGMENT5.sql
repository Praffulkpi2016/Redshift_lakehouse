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

drop table if exists bec_dwh.DIM_SEGMENT5;

create table bec_dwh.DIM_SEGMENT5 diststyle all sortkey(ledger_id,chart_of_accounts_id,flex_value_set_id)
as
(
select distinct 
	s1.chart_of_accounts_id as chart_of_accounts_id,
	s1.flex_value_set_id as flex_value_set_id,
	s1.ledger_id as ledger_id,
	s1.ledger_name as ledger_name,
	concat(s1.chart_of_accounts_id, concat('-', f1.flex_value)) as coa_segval,
	f1.flex_value as level0_value,
	f1.description as level0_desc,
	concat(f1.flex_value, concat('-',f1.description)) as seg5_value_desc,
	s1.segment_name as segment_name,
	f1.creation_date as creation_date,
	f1.last_update_date as last_update,
	-- audit column
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||
		nvl(s1.ledger_id,0)||'-'||nvl(s1.chart_of_accounts_id,0)||'-'||nvl(s1.flex_value_set_id,0)
		||'-'||nvl(f1.flex_value,'0')||'-'||nvl(s1.segment_name,'0') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from 
(
	select 
		f1.flex_value_set_id,
		f1.flex_value_id,
		f1.flex_value,
		f1.creation_date,
		f1.last_update_date,
		f2.description
	from bec_ods.fnd_flex_values f1 
	inner join bec_ods.fnd_flex_values_tl f2 
	on
		f1.flex_value_id = f2.flex_value_id
		and f2."language" = 'US'
	where f1.is_deleted_flg<>'Y'
) f1 
inner join 
(
	select 
		f1.application_id,
		g1.ledger_id,
		f1.id_flex_num,
		f1.id_flex_structure_code,
		f1.id_flex_structure_name,
		f1.description,
		f2.application_column_name,
		f2.segment_name,
		f2.segment_num,
		f2.flex_value_set_id,
		g1.chart_of_accounts_id,
		g1.name as ledger_name
	from bec_ods.fnd_id_flex_structures_vl f1 
	inner join bec_ods.fnd_id_flex_segments f2 
	on
		f1.id_flex_num = f2.id_flex_num 
		and f1.id_flex_code = f2.id_flex_code
		and f2.id_flex_code = 'GL#'
	inner join bec_ods.gl_ledgers g1 
	on
		f1.id_flex_num = g1.chart_of_accounts_id
		where f1.is_deleted_flg<>'Y'
)  s1
on 
	f1.flex_value_set_id = s1.flex_value_set_id
	and s1.application_column_name = 'SEGMENT5');
 

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_segment5'
	and batch_name = 'gl';

commit;
