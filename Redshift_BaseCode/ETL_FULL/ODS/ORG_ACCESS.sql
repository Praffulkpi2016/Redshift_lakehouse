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
	
	DROP TABLE if exists bec_ods.ORG_ACCESS;
	
	CREATE TABLE IF NOT EXISTS bec_ods.ORG_ACCESS
	(
		
		organization_id NUMERIC(15,0)   ENCODE az64
		,resp_application_id NUMERIC(15,0)   ENCODE az64
		,responsibility_id NUMERIC(15,0)   ENCODE az64
		,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,last_updated_by NUMERIC(15,0)   ENCODE az64
		,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,created_by NUMERIC(15,0)   ENCODE az64
		,last_update_login NUMERIC(15,0)   ENCODE az64
		,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,comments VARCHAR(240)   ENCODE lzo
		,request_id NUMERIC(15,0)   ENCODE az64
		,program_application_id NUMERIC(15,0)   ENCODE az64
		,program_id NUMERIC(15,0)   ENCODE az64
		,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,KCA_OPERATION VARCHAR(10)   ENCODE lzo
		,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
		,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE
	auto;
	
	INSERT INTO bec_ods.ORG_ACCESS (
		organization_id,
		resp_application_id,
		responsibility_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		disable_date,
		comments,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		KCA_OPERATION,
		IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
	)
    SELECT
	organization_id,
	resp_application_id,
	responsibility_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	comments,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
	bec_ods_stg.ORG_ACCESS;
	
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
ods_table_name = 'org_access';

Commit;