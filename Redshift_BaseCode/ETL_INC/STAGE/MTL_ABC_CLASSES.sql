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
	
	TRUNCATE TABLE bec_ods_stg.MTL_ABC_CLASSES;
	
	insert into	bec_ods_stg.MTL_ABC_CLASSES
    (
	abc_class_id 
	,abc_class_name 
	,organization_id 
	,last_update_date 
	,last_updated_by 
	,creation_date 
	,created_by 
	,description
	,disable_date 
	,request_id 
	,program_application_id 
	,program_id 
	,program_update_date,
		KCA_OPERATION,
		kca_seq_id,
	kca_seq_date)
	(select
	abc_class_id 
	,abc_class_name 
	,organization_id 
	,last_update_date 
	,last_updated_by 
	,creation_date 
	,created_by 
	,description
	,disable_date 
	,request_id 
	,program_application_id 
	,program_id 
	,program_update_date,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
		from
		bec_raw_dl_ext.MTL_ABC_CLASSES 
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
		and (nvl(ABC_CLASS_ID, 0) ,KCA_SEQ_ID) in 
		(select nvl(ABC_CLASS_ID, 0) as ABC_CLASS_ID  ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MTL_ABC_CLASSES 
			where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
			group by nvl(ABC_CLASS_ID, 0)  )
		and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_abc_classes')	
	);
	END;	