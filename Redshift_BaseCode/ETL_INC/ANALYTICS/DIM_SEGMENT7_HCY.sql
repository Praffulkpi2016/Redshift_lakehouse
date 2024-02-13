/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/


begin;

drop table if exists bec_dwh.DIM_SEGMENT7_HCY;

create table bec_dwh.DIM_SEGMENT7_HCY diststyle all sortkey(structure,l1_value,l2_value,l3_value,l4_value,l5_value,l6_value,l7_value,l8_value,l9_value,l10_value)
as

(
select  DISTINCT
	fnd.structure as structure,
	l1_vsid as l1_vsid,
	l1_value as l1_value,
	l1_description as l1_description,
	l2_value as l2_value,
	l2_description as l2_description,
	l3_value as l3_value,
	l3_description as l3_description,
	l4_value as l4_value,
	l4_description as l4_description,
	l5_value as l5_value,
	l5_description as l5_description,
	l6_value as l6_value,
	l6_description as l6_description,
	l7_value as l7_value,
	l7_description as l7_description,
	l8_value as l8_value,
	l8_description as l8_description,
	l9_value as l9_value,
	l9_description as l9_description,
	l10_value as l10_value,
	l10_description as l10_description,
	-- audit columns
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(fnd.structure,'0')
		||'-'||nvl(l1_vsid,0)||'-'||nvl(l1_value,'0')||'-'||nvl(l2_value,'0')||'-'||nvl(l3_value,'0')||'-'||
		nvl(l4_value,'0')||'-'||nvl(l5_value,'0')||'-'||nvl(l6_value,'0')||'-'||nvl(l7_value,'0')||'-'||
		nvl(l8_value,'0')||'-'||nvl(l9_value,'0')||'-'||nvl(l10_value,'0') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date	   
from (select fifs.flex_value_set_id vsid,
             fifst.id_flex_structure_code structure
      from (SELECT * FROM bec_ods.fnd_id_flex_segments where is_deleted_flg <> 'Y') fifs 
	  inner join bec_ods.fnd_id_flex_structures fifst on fifs.id_flex_num = fifst.id_flex_num
      where fifs.id_flex_code = 'GL#'
      and fifst.id_flex_code = 'GL#'
      and fifs.segment_num = 7) fnd
	  
  join (select h1.flex_value_set_id l1_vsid,
               h1.parent_flex_value l1_value,
               ffvt.description l1_description,
               h1.child_flex_value l2_value,
               ffvt2.description l2_description
        from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
          left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
            on h1.parent_flex_value = h2.child_flex_value
           and h1.flex_value_set_id = h2.flex_value_set_id
          join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
            on ffv.flex_value_set_id = h1.flex_value_set_id
           and ffv.flex_value = h1.parent_flex_value
          join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
            on ffvt.flex_value_id = ffv.flex_value_id
           and ffvt.language = 'US'
          join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv2
            on ffv2.flex_value_set_id = h1.flex_value_set_id
           and ffv2.flex_value = h1.child_flex_value
          join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt2
            on ffvt2.flex_value_id = ffv2.flex_value_id
           and ffvt2.language = 'US') on fnd.vsid = l1_vsid
--level 3

  left outer join (select h1.flex_value_set_id l3_vsid,
                          h1.parent_flex_value l3_parent,
                          h1.child_flex_value l3_value,
                          ffvt.description l3_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l3
               on fnd.vsid = l3_vsid
              and l2_value = l3_parent
--level 4

  left outer join (select h1.flex_value_set_id l4_vsid,
                          h1.parent_flex_value l4_parent,
                          h1.child_flex_value l4_value,
                          ffvt.description l4_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l4
               on fnd.vsid = l4_vsid
              and l3_value = l4_parent
--level 5

  left outer join (select h1.flex_value_set_id l5_vsid,
                          h1.parent_flex_value l5_parent,
                          h1.child_flex_value l5_value,
                          ffvt.description l5_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l5
               on fnd.vsid = l5_vsid
              and l4_value = l5_parent
--level 6

  left outer join (select h1.flex_value_set_id l6_vsid,
                          h1.parent_flex_value l6_parent,
                          h1.child_flex_value l6_value,
                          ffvt.description l6_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l6
               on fnd.vsid = l6_vsid
              and l5_value = l6_parent
--level 7

  left outer join (select h1.flex_value_set_id l7_vsid,
                          h1.parent_flex_value l7_parent,
                          h1.child_flex_value l7_value,
                          ffvt.description l7_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l7
               on fnd.vsid = l7_vsid
              and l6_value = l7_parent
--level 8

  left outer join (select h1.flex_value_set_id l8_vsid,
                          h1.parent_flex_value l8_parent,
                          h1.child_flex_value l8_value,
                          ffvt.description l8_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l8
               on fnd.vsid = l8_vsid
              and l7_value = l8_parent
--level 9

  left outer join (select h1.flex_value_set_id l9_vsid,
                          h1.parent_flex_value l9_parent,
                          h1.child_flex_value l9_value,
                          ffvt.description l9_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l9
               on fnd.vsid = l9_vsid
              and l8_value = l9_parent
--level 10

  left outer join (select h1.flex_value_set_id l10_vsid,
                          h1.parent_flex_value l10_parent,
                          h1.child_flex_value l10_value,
                          ffvt.description l10_description
                   from (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h1
                     left outer join (SELECT * FROM bec_ods.gl_seg_val_norm_hierarchy where is_deleted_flg <> 'Y') h2
                       on h1.parent_flex_value = h2.child_flex_value
                      and h1.flex_value_set_id = h2.flex_value_set_id
                     join (SELECT * FROM bec_ods.fnd_flex_values where is_deleted_flg <> 'Y') ffv
                       on ffv.flex_value_set_id = h1.flex_value_set_id
                      and ffv.flex_value = h1.child_flex_value
                     join (SELECT * FROM bec_ods.fnd_flex_values_tl where is_deleted_flg <> 'Y')  ffvt
                       on ffvt.flex_value_id = ffv.flex_value_id
                      and ffvt.language = 'US') l10
               on fnd.vsid = l10_vsid
              and l9_value = l10_parent);  
 

end;

 

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_segment7_hcy'
	and batch_name = 'gl';

commit;