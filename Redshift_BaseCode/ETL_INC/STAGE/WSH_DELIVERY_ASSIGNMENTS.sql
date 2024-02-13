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

truncate
	table bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS;

insert
	into
	bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS
    (delivery_assignment_id,
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
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE)
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
		KCA_OPERATION,
		KCA_SEQ_ID
		,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		and (nvl(DELIVERY_ASSIGNMENT_ID, 0), 
		KCA_SEQ_ID) in 
	(
		select
			nvl(DELIVERY_ASSIGNMENT_ID, 0) as DELIVERY_ASSIGNMENT_ID ,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		group by
			DELIVERY_ASSIGNMENT_ID )
		and (KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wsh_delivery_assignments')
)
);
end;