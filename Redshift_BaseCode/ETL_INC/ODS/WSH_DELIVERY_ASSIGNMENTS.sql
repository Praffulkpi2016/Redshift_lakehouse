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

delete
from
	bec_ods.WSH_DELIVERY_ASSIGNMENTS
where
	(
	nvl(DELIVERY_ASSIGNMENT_ID, 0) 
	) in 
	(
	select
		NVL(stg.DELIVERY_ASSIGNMENT_ID, 0) as DELIVERY_ASSIGNMENT_ID 
	from
		bec_ods.WSH_DELIVERY_ASSIGNMENTS ods,
		bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS stg
	where
		    NVL(ods.DELIVERY_ASSIGNMENT_ID, 0) = NVL(stg.DELIVERY_ASSIGNMENT_ID, 0) 
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.WSH_DELIVERY_ASSIGNMENTS (
		delivery_assignment_id,
	delivery_id,
	parent_delivery_id,
	delivery_detail_id,
	parent_delivery_detail_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	active_flag,
	"type",
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date)
(
	select
	delivery_assignment_id,
	delivery_id,
	parent_delivery_id,
	delivery_detail_id,
	parent_delivery_detail_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	active_flag,
	"type",
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(DELIVERY_ASSIGNMENT_ID, 0) ,
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(DELIVERY_ASSIGNMENT_ID, 0) as DELIVERY_ASSIGNMENT_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			DELIVERY_ASSIGNMENT_ID 
			)	
	);

commit;

-- Soft delete
update bec_ods.WSH_DELIVERY_ASSIGNMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WSH_DELIVERY_ASSIGNMENTS set IS_DELETED_FLG = 'Y'
where (DELIVERY_ASSIGNMENT_ID)  in
(
select DELIVERY_ASSIGNMENT_ID from bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
where (DELIVERY_ASSIGNMENT_ID,KCA_SEQ_ID)
in 
(
select DELIVERY_ASSIGNMENT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
group by DELIVERY_ASSIGNMENT_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'wsh_delivery_assignments';

commit;