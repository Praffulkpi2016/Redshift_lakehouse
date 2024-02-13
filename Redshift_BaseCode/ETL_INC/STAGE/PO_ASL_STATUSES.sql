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
BEGIN;
	
	TRUNCATE TABLE bec_ods_stg.PO_ASL_STATUSES;
	
	insert into	bec_ods_stg.PO_ASL_STATUSES
    (
		status_id,
		status,
		status_description,
		asl_default_flag,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		inactive_date,
		attribute_category,
		attribute1,
		attribute2,
		attribute3,
		attribute4,
		attribute5,
		attribute6,
		attribute7,
		attribute8,
		attribute9,
		attribute10,
		attribute11,
		attribute12,
		attribute13,
		attribute14,
		attribute15,
		last_update_login,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		zd_edition_name,
		zd_sync,
		KCA_OPERATION,
		kca_seq_id,
	kca_seq_date)
	(select
		status_id,
		status,
		status_description,
		asl_default_flag,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		inactive_date,
		attribute_category,
		attribute1,
		attribute2,
		attribute3,
		attribute4,
		attribute5,
		attribute6,
		attribute7,
		attribute8,
		attribute9,
		attribute10,
		attribute11,
		attribute12,
		attribute13,
		attribute14,
		attribute15,
		last_update_login,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		zd_edition_name,
		zd_sync,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
		from
		bec_raw_dl_ext.PO_ASL_STATUSES 
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
		and (nvl(STATUS_ID, 0) ,KCA_SEQ_ID) in 
		(select nvl(STATUS_ID, 0) as STATUS_ID  ,max(KCA_SEQ_ID) from bec_raw_dl_ext.PO_ASL_STATUSES 
			where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
			group by nvl(STATUS_ID, 0)  )
		and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='po_asl_statuses')	
	);
	END;	