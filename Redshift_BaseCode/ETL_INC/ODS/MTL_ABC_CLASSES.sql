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
	bec_ods.MTL_ABC_CLASSES
	where
	(
		nvl(ABC_CLASS_ID, 0)  
		
	) in 
	(
		select
		NVL(stg.ABC_CLASS_ID,0) AS ABC_CLASS_ID 
		from
		bec_ods.mtl_abc_classes ods,
		bec_ods_stg.mtl_abc_classes stg
		where
		NVL(ods.ABC_CLASS_ID,0) = NVL(stg.ABC_CLASS_ID,0)   
		and stg.kca_operation in ('INSERT', 'UPDATE')
	);
	
	commit;
	-- Insert records
	
	insert
	into bec_ods.mtl_abc_classes  (
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
		kca_operation,
		IS_DELETED_FLG,
		kca_seq_id,
	kca_seq_date)
	(
		select
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
		from
		bec_ods_stg.mtl_abc_classes
		where
		kca_operation IN ('INSERT','UPDATE')
		and (
			nvl(ABC_CLASS_ID, 0) ,
			KCA_SEQ_ID
		) in 
		(
			select
			nvl(ABC_CLASS_ID, 0) as ABC_CLASS_ID  ,
			max(KCA_SEQ_ID)
			from
			bec_ods_stg.mtl_abc_classes
			where
			kca_operation IN ('INSERT','UPDATE')
			group by
			nvl(ABC_CLASS_ID, 0)  
		)	
	);
	
	commit;
	
	-- Soft delete
update bec_ods.mtl_abc_classes set IS_DELETED_FLG = 'N';
commit;
update bec_ods.mtl_abc_classes set IS_DELETED_FLG = 'Y'
where (ABC_CLASS_ID)  in
(
select ABC_CLASS_ID from bec_raw_dl_ext.mtl_abc_classes
where (ABC_CLASS_ID,KCA_SEQ_ID)
in 
(
select ABC_CLASS_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.mtl_abc_classes
group by ABC_CLASS_ID
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
ods_table_name = 'mtl_abc_classes';

commit;	