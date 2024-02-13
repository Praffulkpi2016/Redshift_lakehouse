/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.OE_DROP_SHIP_SOURCES
where (HEADER_ID,LINE_ID) in (
select stg.HEADER_ID,stg.LINE_ID
from bec_ods.OE_DROP_SHIP_SOURCES ods, bec_ods_stg.OE_DROP_SHIP_SOURCES stg
where ods.HEADER_ID = stg.HEADER_ID
and  ods.LINE_ID = stg.LINE_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OE_DROP_SHIP_SOURCES
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
	is_deleted_flg,
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.OE_DROP_SHIP_SOURCES
	where kca_operation in ('INSERT','UPDATE') 
	and (HEADER_ID,LINE_ID,kca_seq_id) in 
	(select HEADER_ID,LINE_ID,max(kca_seq_id) from bec_ods_stg.OE_DROP_SHIP_SOURCES 
     where kca_operation in ('INSERT','UPDATE')
     group by HEADER_ID,LINE_ID)
);

commit;



-- Soft delete
update bec_ods.OE_DROP_SHIP_SOURCES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OE_DROP_SHIP_SOURCES set IS_DELETED_FLG = 'Y'
where (HEADER_ID,LINE_ID)  in
(
select HEADER_ID,LINE_ID from bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
where (HEADER_ID,LINE_ID,KCA_SEQ_ID)
in 
(
select HEADER_ID,LINE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
group by HEADER_ID,LINE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oe_drop_ship_sources';

commit;