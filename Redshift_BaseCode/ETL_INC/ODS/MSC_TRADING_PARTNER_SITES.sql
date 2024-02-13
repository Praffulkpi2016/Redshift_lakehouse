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

delete from bec_ods.MSC_TRADING_PARTNER_SITES
where (nvl(PARTNER_SITE_ID,0)) in (
select nvl(stg.PARTNER_SITE_ID,0) as PARTNER_SITE_ID
 
from bec_ods.MSC_TRADING_PARTNER_SITES ods, bec_ods_stg.MSC_TRADING_PARTNER_SITES stg
where nvl(ods.PARTNER_SITE_ID,0) = nvl(stg.PARTNER_SITE_ID,0) AND
 stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MSC_TRADING_PARTNER_SITES
    (  partner_site_id,
	sr_tp_site_id,
	sr_instance_id,
	partner_id,
	partner_address,
	tp_site_code,
	sr_tp_id,
	"location",
	partner_type,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	operating_unit_name,
	country,
	state,
	city,
	postal_code,
	shipping_control,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	 partner_site_id,
	sr_tp_site_id,
	sr_instance_id,
	partner_id,
	partner_address,
	tp_site_code,
	sr_tp_id,
	"location",
	partner_type,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	operating_unit_name,
	country,
	state,
	city,
	postal_code,
	shipping_control,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.MSC_TRADING_PARTNER_SITES
where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(PARTNER_SITE_ID,0),KCA_SEQ_ID) in 
	(select nvl(PARTNER_SITE_ID,0) AS PARTNER_SITE_ID,max(KCA_SEQ_ID) from bec_ods_stg.MSC_TRADING_PARTNER_SITES 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(PARTNER_SITE_ID,0))	
	);

commit;

 

-- Soft delete
update bec_ods.MSC_TRADING_PARTNER_SITES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_TRADING_PARTNER_SITES set IS_DELETED_FLG = 'Y'
where (PARTNER_SITE_ID)  in
(
select PARTNER_SITE_ID from bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
where (PARTNER_SITE_ID,KCA_SEQ_ID)
in 
(
select PARTNER_SITE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
group by PARTNER_SITE_ID
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
	ods_table_name = 'msc_trading_partner_sites';

commit;