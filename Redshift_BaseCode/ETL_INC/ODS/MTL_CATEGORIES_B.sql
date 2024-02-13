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

delete from bec_ods.mtl_categories_b
where CATEGORY_ID in (
select stg.CATEGORY_ID from bec_ods.mtl_categories_b ods, bec_ods_stg.mtl_categories_b stg
where ods.CATEGORY_ID = stg.CATEGORY_ID and stg.kca_operation in ('INSERT','UPDATE'));

commit;

-- Insert records

insert into bec_ods.mtl_categories_b
(CATEGORY_ID,
STRUCTURE_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
DESCRIPTION,
DISABLE_DATE,
SEGMENT1,
SEGMENT2,
SEGMENT3,
SEGMENT4,
SEGMENT5,
SEGMENT6,
SEGMENT7,
SEGMENT8,
SEGMENT9,
SEGMENT10,
SEGMENT11,
SEGMENT12,
SEGMENT13,
SEGMENT14,
SEGMENT15,
SEGMENT16,
SEGMENT17,
SEGMENT18,
SEGMENT19,
SEGMENT20,
SUMMARY_FLAG,
ENABLED_FLAG,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
--TOTAL_PROD_ID,
WEB_STATUS,
SUPPLIER_ENABLED_FLAG,
ZD_EDITION_NAME,
ZD_SYNC,
KCA_OPERATION,
IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date)
(
select CATEGORY_ID,
STRUCTURE_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
DESCRIPTION,
DISABLE_DATE,
SEGMENT1,
SEGMENT2,
SEGMENT3,
SEGMENT4,
SEGMENT5,
SEGMENT6,
SEGMENT7,
SEGMENT8,
SEGMENT9,
SEGMENT10,
SEGMENT11,
SEGMENT12,
SEGMENT13,
SEGMENT14,
SEGMENT15,
SEGMENT16,
SEGMENT17,
SEGMENT18,
SEGMENT19,
SEGMENT20,
SUMMARY_FLAG,
ENABLED_FLAG,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
--TOTAL_PROD_ID,
WEB_STATUS,
SUPPLIER_ENABLED_FLAG,
ZD_EDITION_NAME,
ZD_SYNC,
KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.mtl_categories_b
where kca_operation IN ('INSERT','UPDATE') and (CATEGORY_ID,kca_seq_id) in (select CATEGORY_ID,max(kca_seq_id) from bec_ods_stg.mtl_categories_b 
where kca_operation IN ('INSERT','UPDATE')
group by CATEGORY_ID)
);

commit;
 

-- Soft delete
update bec_ods.mtl_categories_b set IS_DELETED_FLG = 'N';
commit;
update bec_ods.mtl_categories_b set IS_DELETED_FLG = 'Y'
where (CATEGORY_ID)  in
(
select CATEGORY_ID from bec_raw_dl_ext.mtl_categories_b
where (CATEGORY_ID,KCA_SEQ_ID)
in 
(
select CATEGORY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.mtl_categories_b
group by CATEGORY_ID
) 
and kca_operation= 'DELETE'
);
commit;
end; 

update bec_etl_ctrl.batch_ods_info set load_type = 'I', last_refresh_date = getdate() where ods_table_name='mtl_categories_b';
commit;