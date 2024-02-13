/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents full load approach for ODS.
	# File Version: KPI v1.0
*/

begin;
	
	DROP TABLE if exists bec_ods.MTL_ABC_CLASSES;
	
	CREATE TABLE IF NOT EXISTS bec_ods.MTL_ABC_CLASSES
	(		
	abc_class_id NUMERIC(15,0)   ENCODE az64
	,abc_class_name VARCHAR(40)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(50)   ENCODE lzo
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,KCA_OPERATION VARCHAR(10)   ENCODE lzo,
		IS_DELETED_FLG VARCHAR(2) ENCODE lzo, 
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE
	auto;
	
	insert
	into bec_ods.MTL_ABC_CLASSES (
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
		IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
	)
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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from
	bec_ods_stg.MTL_ABC_CLASSES;
end;

update
bec_etl_ctrl.batch_ods_info
set
load_type = 'I',
last_refresh_date = getdate()
where
ods_table_name = 'mtl_abc_classes';

commit;