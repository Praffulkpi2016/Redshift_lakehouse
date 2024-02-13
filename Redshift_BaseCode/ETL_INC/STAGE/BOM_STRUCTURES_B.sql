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
begin;

truncate table bec_ods_stg.BOM_STRUCTURES_B;

insert into	bec_ods_stg.bom_structures_b
   (
   assembly_item_id
,organization_id
,alternate_bom_designator
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,common_assembly_item_id
,specific_assembly_comment
,pending_from_ecn
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
,assembly_type
,common_bill_sequence_id
,bill_sequence_id
,request_id
,program_application_id
,program_id
,program_update_date
,common_organization_id
,next_explode_date
,project_id
,task_id
,original_system_reference
,structure_type_id
,implementation_date
,obj_name
,pk1_value
,pk2_value
,pk3_value
,pk4_value
,pk5_value
,effectivity_control
,is_preferred
,source_bill_sequence_id
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
	)
(
	select
assembly_item_id
,organization_id
,alternate_bom_designator
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,common_assembly_item_id
,specific_assembly_comment
,pending_from_ecn
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
,assembly_type
,common_bill_sequence_id
,bill_sequence_id
,request_id
,program_application_id
,program_id
,program_update_date
,common_organization_id
,next_explode_date
,project_id
,task_id
,original_system_reference
,structure_type_id
,implementation_date
,obj_name
,pk1_value
,pk2_value
,pk3_value
,pk4_value
,pk5_value
,effectivity_control
,is_preferred
,source_bill_sequence_id
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
	from bec_raw_dl_ext.bom_structures_b
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (BILL_SEQUENCE_ID,kca_seq_id) in 
	(select BILL_SEQUENCE_ID,max(kca_seq_id) from bec_raw_dl_ext.bom_structures_b 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by BILL_SEQUENCE_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'bom_structures_b')
);
end;