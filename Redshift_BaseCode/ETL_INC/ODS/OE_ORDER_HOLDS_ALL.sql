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

delete from bec_ods.OE_ORDER_HOLDS_ALL
where nvl(ORDER_HOLD_ID,0) in (
select nvl(stg.ORDER_HOLD_ID,0) as ORDER_HOLD_ID from bec_ods.OE_ORDER_HOLDS_ALL ods, bec_ods_stg.OE_ORDER_HOLDS_ALL stg
where nvl(ods.ORDER_HOLD_ID,0) = nvl(stg.ORDER_HOLD_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OE_ORDER_HOLDS_ALL
       (	
		ORDER_HOLD_ID
       ,CREATION_DATE
       ,CREATED_BY
       ,LAST_UPDATE_DATE
       ,LAST_UPDATED_BY
       ,LAST_UPDATE_LOGIN
       ,PROGRAM_APPLICATION_ID
       ,PROGRAM_ID
       ,PROGRAM_UPDATE_DATE
       ,REQUEST_ID
       ,HOLD_SOURCE_ID
       ,HOLD_RELEASE_ID
       ,HEADER_ID
       ,LINE_ID
       ,ORG_ID
       ,CONTEXT
       ,ATTRIBUTE1
       ,ATTRIBUTE2
       ,ATTRIBUTE3
       ,ATTRIBUTE4
       ,ATTRIBUTE5
       ,ATTRIBUTE6
       ,ATTRIBUTE7
       ,ATTRIBUTE8
       ,ATTRIBUTE9
       ,ATTRIBUTE10
       ,ATTRIBUTE11
       ,ATTRIBUTE12
       ,ATTRIBUTE13
       ,ATTRIBUTE14
       ,ATTRIBUTE15
       ,RELEASED_FLAG
       ,INST_ID
       ,CREDIT_PROFILE_LEVEL,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
(
	select
		ORDER_HOLD_ID
       ,CREATION_DATE
       ,CREATED_BY
       ,LAST_UPDATE_DATE
       ,LAST_UPDATED_BY
       ,LAST_UPDATE_LOGIN
       ,PROGRAM_APPLICATION_ID
       ,PROGRAM_ID
       ,PROGRAM_UPDATE_DATE
       ,REQUEST_ID
       ,HOLD_SOURCE_ID
       ,HOLD_RELEASE_ID
       ,HEADER_ID
       ,LINE_ID
       ,ORG_ID
       ,CONTEXT
       ,ATTRIBUTE1
       ,ATTRIBUTE2
       ,ATTRIBUTE3
       ,ATTRIBUTE4
       ,ATTRIBUTE5
       ,ATTRIBUTE6
       ,ATTRIBUTE7
       ,ATTRIBUTE8
       ,ATTRIBUTE9
       ,ATTRIBUTE10
       ,ATTRIBUTE11
       ,ATTRIBUTE12
       ,ATTRIBUTE13
       ,ATTRIBUTE14
       ,ATTRIBUTE15
       ,RELEASED_FLAG
       ,INST_ID
       ,CREDIT_PROFILE_LEVEL,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.OE_ORDER_HOLDS_ALL
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(ORDER_HOLD_ID,0),kca_seq_id) in 
	(select nvl(ORDER_HOLD_ID,0) as ORDER_HOLD_ID,max(kca_seq_id) from bec_ods_stg.OE_ORDER_HOLDS_ALL 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(ORDER_HOLD_ID,0))
);

commit;



-- Soft delete
update bec_ods.OE_ORDER_HOLDS_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OE_ORDER_HOLDS_ALL set IS_DELETED_FLG = 'Y'
where (ORDER_HOLD_ID)  in
(
select ORDER_HOLD_ID from bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
where (ORDER_HOLD_ID,KCA_SEQ_ID)
in 
(
select ORDER_HOLD_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
group by ORDER_HOLD_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oe_order_holds_all';

commit;