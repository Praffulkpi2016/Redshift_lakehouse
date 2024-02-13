/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_ods_stg.MTL_TRANSACTION_LOT_NUMBERS;

create table bec_ods_stg.MTL_TRANSACTION_LOT_NUMBERS as 
select * from
	bec_raw_dl_ext.MTL_TRANSACTION_LOT_NUMBERS
where
	kca_operation != 'DELETE';
	
end;