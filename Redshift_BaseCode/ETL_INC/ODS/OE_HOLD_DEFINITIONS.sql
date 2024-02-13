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
	
	delete from bec_ods.oe_hold_definitions
	where (nvl(HOLD_ID,0)) in (
		select nvl(stg.HOLD_ID,0) as HOLD_ID from bec_ods.oe_hold_definitions ods, bec_ods_stg.oe_hold_definitions stg
		where nvl(ods.HOLD_ID,0) = nvl(stg.HOLD_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
	);
	
	commit;
	
	-- Insert records
	
	insert into	bec_ods.oe_hold_definitions
	(	
		HOLD_ID,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		NAME,
		TYPE_CODE,
		DESCRIPTION,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		ITEM_TYPE,
		ACTIVITY_NAME,
		CONTEXT,
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
		HOLD_INCLUDED_ITEMS_FLAG,
		APPLY_TO_ORDER_AND_LINE_FLAG,
		PROGRESS_WF_ON_RELEASE_FLAG,
		ZD_EDITION_NAME,
		ZD_SYNC, 
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
	,kca_seq_date)	
	(
		select
		HOLD_ID,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		NAME,
		TYPE_CODE,
		DESCRIPTION,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		ITEM_TYPE,
		ACTIVITY_NAME,
		CONTEXT,
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
		HOLD_INCLUDED_ITEMS_FLAG,
		APPLY_TO_ORDER_AND_LINE_FLAG,
		PROGRESS_WF_ON_RELEASE_FLAG,
		ZD_EDITION_NAME,
		ZD_SYNC,
        KCA_OPERATION,
		'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
		from bec_ods_stg.oe_hold_definitions
		where kca_operation in ('INSERT','UPDATE') 
		and (nvl(HOLD_ID,0),kca_seq_id) in 
		(select nvl(HOLD_ID,0) as HOLD_ID,max(kca_seq_id) from bec_ods_stg.oe_hold_definitions 
			where kca_operation in ('INSERT','UPDATE')
		group by nvl(HOLD_ID,0))
	);
	
	commit;
	
	
	
	-- Soft delete
update bec_ods.oe_hold_definitions set IS_DELETED_FLG = 'N';
commit;
update bec_ods.oe_hold_definitions set IS_DELETED_FLG = 'Y'
where (HOLD_ID)  in
(
select HOLD_ID from bec_raw_dl_ext.oe_hold_definitions
where (HOLD_ID,KCA_SEQ_ID)
in 
(
select HOLD_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.oe_hold_definitions
group by HOLD_ID
) 
and kca_operation= 'DELETE'
);
commit;
	
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oe_hold_definitions';

commit;