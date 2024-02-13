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

delete from bec_ods.MTL_UOM_CONVERSIONS
where (INVENTORY_ITEM_ID,UNIT_OF_MEASURE) in (
select stg.INVENTORY_ITEM_ID,stg.UNIT_OF_MEASURE
from bec_ods.MTL_UOM_CONVERSIONS ods, bec_ods_stg.MTL_UOM_CONVERSIONS stg
where ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
and  ods.UNIT_OF_MEASURE = stg.UNIT_OF_MEASURE
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_UOM_CONVERSIONS
       (	unit_of_measure,
	uom_code,
	uom_class,
	inventory_item_id,
	conversion_rate,
	default_conversion_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	length,
	width,
	height,
	dimension_uom,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
			unit_of_measure,
	uom_code,
	uom_class,
	inventory_item_id,
	conversion_rate,
	default_conversion_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	length,
	width,
	height,
	dimension_uom,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.MTL_UOM_CONVERSIONS
	where kca_operation in ('INSERT','UPDATE') 
	and (INVENTORY_ITEM_ID,UNIT_OF_MEASURE,kca_seq_id) in 
	(select INVENTORY_ITEM_ID,UNIT_OF_MEASURE,max(kca_seq_id) from bec_ods_stg.MTL_UOM_CONVERSIONS 
     where kca_operation in ('INSERT','UPDATE')
     group by INVENTORY_ITEM_ID,UNIT_OF_MEASURE)
);

commit;



-- Soft delete
update bec_ods.MTL_UOM_CONVERSIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_UOM_CONVERSIONS set IS_DELETED_FLG = 'Y'
where (INVENTORY_ITEM_ID,UNIT_OF_MEASURE)  in
(
select INVENTORY_ITEM_ID,UNIT_OF_MEASURE from bec_raw_dl_ext.MTL_UOM_CONVERSIONS
where (INVENTORY_ITEM_ID,UNIT_OF_MEASURE,KCA_SEQ_ID)
in 
(
select INVENTORY_ITEM_ID,UNIT_OF_MEASURE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_UOM_CONVERSIONS
group by INVENTORY_ITEM_ID,UNIT_OF_MEASURE
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_uom_conversions';

commit;