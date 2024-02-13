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

delete from bec_ods.PO_ASL_DOCUMENTS
where (nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0))  in (
select nvl(stg.ASL_ID, 0) as ASL_ID,
            nvl(stg.USING_ORGANIZATION_ID, 0) as USING_ORGANIZATION_ID,
            nvl(stg.DOCUMENT_HEADER_ID, 0) as DOCUMENT_HEADER_ID
             from bec_ods.PO_ASL_DOCUMENTS ods, bec_ods_stg.PO_ASL_DOCUMENTS stg
where nvl(ods.ASL_ID,0) = nvl(stg.ASL_ID,0)
and nvl(ods.USING_ORGANIZATION_ID,0) = nvl(stg.USING_ORGANIZATION_ID,0)
and nvl(ods.DOCUMENT_HEADER_ID,0) = nvl(stg.DOCUMENT_HEADER_ID,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.PO_ASL_DOCUMENTS
       (asl_id
    ,using_organization_id
    ,sequence_num
    ,document_type_code
    ,document_header_id
    ,document_line_id
    ,last_update_date
    ,last_updated_by
    ,creation_date
    ,created_by
    ,attribute_category
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15
    ,last_update_login
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,org_id,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)
(
	select
		asl_id
    ,using_organization_id
    ,sequence_num
    ,document_type_code
    ,document_header_id
    ,document_line_id
    ,last_update_date
    ,last_updated_by
    ,creation_date
    ,created_by
    ,attribute_category
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15
    ,last_update_login
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,org_id,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.PO_ASL_DOCUMENTS
	where kca_operation IN ('INSERT','UPDATE')
	and (nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0),kca_seq_id) in
	(select nvl(ASL_ID, 0) as ASL_ID,
            nvl(USING_ORGANIZATION_ID, 0) as USING_ORGANIZATION_ID,
            nvl(DOCUMENT_HEADER_ID, 0) as DOCUMENT_HEADER_ID,
	max(kca_seq_id) from bec_ods_stg.PO_ASL_DOCUMENTS
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(ASL_ID, 0) ,
		nvl(USING_ORGANIZATION_ID, 0) ,
        nvl(DOCUMENT_HEADER_ID, 0))
);

commit;

-- Soft delete
update bec_ods.PO_ASL_DOCUMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.PO_ASL_DOCUMENTS set IS_DELETED_FLG = 'Y'
where (nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0))  in
(
select nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0) from bec_raw_dl_ext.PO_ASL_DOCUMENTS
where (nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0),KCA_SEQ_ID)
in 
(
select nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.PO_ASL_DOCUMENTS
group by nvl(ASL_ID, 0), nvl(USING_ORGANIZATION_ID, 0),nvl(DOCUMENT_HEADER_ID, 0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'po_asl_documents';

commit;