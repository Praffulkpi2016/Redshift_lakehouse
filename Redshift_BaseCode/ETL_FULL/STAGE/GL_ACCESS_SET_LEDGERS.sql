/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_ods_stg.GL_ACCESS_SET_LEDGERS;

create table bec_ods_stg.GL_ACCESS_SET_LEDGERS as 
select * from
	bec_raw_dl_ext.GL_ACCESS_SET_LEDGERS
	where
	kca_operation != 'DELETE';
	
end;