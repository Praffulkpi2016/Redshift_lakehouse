/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;
truncate table bec_ods_stg.OE_DROP_SHIP_SOURCES;
insert into	bec_ods_stg.OE_DROP_SHIP_SOURCES
   (drop_ship_source_id,
	header_id,
	line_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	org_id,
	destination_organization_id,
	requisition_header_id,
	requisition_line_id,
	po_header_id,
	po_line_id,
	line_location_id,
	po_release_id,
	inst_id,
	kca_operation,
	kca_seq_id
	,kca_seq_date)
(
	select
	drop_ship_source_id,
	header_id,
	line_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	org_id,
	destination_organization_id,
	requisition_header_id,
	requisition_line_id,
	po_header_id,
	po_line_id,
	line_location_id,
	po_release_id,
	inst_id,
	kca_operation,
	kca_seq_id
	,kca_seq_date
	from bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
	where kca_operation != 'DELETE'   and nvl(kca_seq_id,'') != ''
	and (HEADER_ID,LINE_ID,kca_seq_id) in 
	(select HEADER_ID,LINE_ID,max(kca_seq_id) from bec_raw_dl_ext.OE_DROP_SHIP_SOURCES 
     where kca_operation != 'DELETE'   and nvl(kca_seq_id,'') != ''
     group by HEADER_ID,LINE_ID)
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oe_drop_ship_sources')
            )	
);
end;