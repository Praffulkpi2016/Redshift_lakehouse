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

TRUNCATE TABLE bec_ods_stg.MSC_COMPANY_SITES ;

insert into	bec_ods_stg.MSC_COMPANY_SITES 
    (  company_site_id,
	company_id,
	company_site_name,
	sr_instance_id,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	disable_date,
	planning_enabled,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
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
	request_id,
	program_id,
	program_update_date,
	"location",
	address1,
	address2,
	address3,
	address4,
	country,
	state,
	city,
	county,
	province,
	postal_code,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date)
(select
	company_site_id,
	company_id,
	company_site_name,
	sr_instance_id,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	disable_date,
	planning_enabled,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
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
	request_id,
	program_id,
	program_update_date,
	"location",
	address1,
	address2,
	address3,
	address4,
	country,
	state,
	city,
	county,
	province,
	postal_code,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date
from
	bec_raw_dl_ext.MSC_COMPANY_SITES  
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(COMPANY_ID,0),nvl(COMPANY_SITE_ID,0) ,KCA_SEQ_ID) in 
	(select nvl(COMPANY_ID,0) as COMPANY_ID ,nvl(COMPANY_SITE_ID,0) AS COMPANY_SITE_ID ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MSC_COMPANY_SITES  
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(COMPANY_ID,0),nvl(COMPANY_SITE_ID,0) )
     and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where 
	 ods_table_name ='msc_company_sites')
	   
            ));
END;