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

TRUNCATE TABLE bec_ods.HR_OPERATING_UNITS;

insert into bec_ods.HR_OPERATING_UNITS
(
business_group_id,
	organization_id,
	"name",
	date_from,
	date_to,
	short_code,
	set_of_books_id,
	default_legal_context_id,
	usable_flag,
kca_operation 
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date)
SELECT
business_group_id,
	organization_id,
	"name",
	date_from,
	date_to,
	short_code,
	set_of_books_id,
	default_legal_context_id,
	usable_flag,
kca_operation 
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
FROM bec_ods_stg.HR_OPERATING_UNITS;

end;
update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'hr_operating_units';
commit;