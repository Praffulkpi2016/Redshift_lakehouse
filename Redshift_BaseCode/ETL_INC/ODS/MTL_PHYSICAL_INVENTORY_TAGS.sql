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

delete from bec_ods.MTL_PHYSICAL_INVENTORY_TAGS
where (nvl(TAG_ID,0)) in (
select nvl(stg.TAG_ID,0) as TAG_ID from bec_ods.MTL_PHYSICAL_INVENTORY_TAGS ods, bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS stg
where nvl(ods.TAG_ID,0) = nvl(stg.TAG_ID,0) 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_PHYSICAL_INVENTORY_TAGS
       (	
		TAG_ID, 
		PHYSICAL_INVENTORY_ID, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		VOID_FLAG, 
		TAG_NUMBER, 
		ADJUSTMENT_ID, 
		INVENTORY_ITEM_ID, 
		TAG_QUANTITY, 
		TAG_UOM, 
		TAG_QUANTITY_AT_STANDARD_UOM, 
		STANDARD_UOM, 
		SUBINVENTORY, 
		LOCATOR_ID, 
		LOT_NUMBER, 
		LOT_EXPIRATION_DATE, 
		REVISION, 
		SERIAL_NUM, 
		COUNTED_BY_EMPLOYEE_ID, 
		LOT_SERIAL_CONTROLS, 
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
		PARENT_LPN_ID, 
		OUTERMOST_LPN_ID, 
		COST_GROUP_ID, 
		TAG_SECONDARY_QUANTITY, 
		TAG_SECONDARY_UOM, 
		TAG_QTY_AT_STD_SECONDARY_UOM, 
		STANDARD_SECONDARY_UOM,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		TAG_ID, 
		PHYSICAL_INVENTORY_ID, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		VOID_FLAG, 
		TAG_NUMBER, 
		ADJUSTMENT_ID, 
		INVENTORY_ITEM_ID, 
		TAG_QUANTITY, 
		TAG_UOM, 
		TAG_QUANTITY_AT_STANDARD_UOM, 
		STANDARD_UOM, 
		SUBINVENTORY, 
		LOCATOR_ID, 
		LOT_NUMBER, 
		LOT_EXPIRATION_DATE, 
		REVISION, 
		SERIAL_NUM, 
		COUNTED_BY_EMPLOYEE_ID, 
		LOT_SERIAL_CONTROLS, 
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
		PARENT_LPN_ID, 
		OUTERMOST_LPN_ID, 
		COST_GROUP_ID, 
		TAG_SECONDARY_QUANTITY, 
		TAG_SECONDARY_UOM, 
		TAG_QTY_AT_STD_SECONDARY_UOM, 
		STANDARD_SECONDARY_UOM,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(TAG_ID,0),kca_seq_ID) in 
	(select nvl(TAG_ID,0) as TAG_ID,max(kca_seq_ID) from bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(TAG_ID,0))
);

commit;

-- Soft delete
update bec_ods.MTL_PHYSICAL_INVENTORY_TAGS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_PHYSICAL_INVENTORY_TAGS set IS_DELETED_FLG = 'Y'
where (TAG_ID )  in
(
select TAG_ID  from bec_raw_dl_ext.MTL_PHYSICAL_INVENTORY_TAGS
where (TAG_ID ,KCA_SEQ_ID)
in 
(
select TAG_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_PHYSICAL_INVENTORY_TAGS
group by TAG_ID 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_physical_inventory_tags';

commit;