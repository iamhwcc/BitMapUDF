# Introduction
Project contains hive UDF/UDAF on BitMap

## UDF/UDAF usage
|   Function Name   | Function Type  |                         Description                          |              Usage               | Return Type (hive) |
|:-----------------:|:--------------:|:------------------------------------------------------------:|:--------------------------------:|:------------------:|
|     to_bitmap     |    **UDAF**    |                  将num(int或bigint)转化为bitmap                   |          to_bitmap(num)          |  bitmap (binary)   |
|   bitmap_union    |    **UDAF**    |                   多个bitmap合并为一个bitmap(并集)                    |       bitmap_union(bitmap)       |  bitmap (binary)   |
|   bitmap_count    |      UDF       |                      计算bitmap中存储的num个数                       |       bitmap_count(bitmap)       |        int         |
|    bitmap_and     |      UDF       |                         计算两个bitmap交集                         |   bitmap_and(bitmap1,bitmap2)    |  bitmap (binary)   |
|     bitmap_or     |      UDF       |                         计算两个bitmap并集                         |    bitmap_or(bitmap1,bitmap2)    |  bitmap (binary)   |
|    bitmap_xor     |      UDF       |                         计算两个bitmap差集                         |   bitmap_xor(bitmap1,bitmap2)    |  bitmap (binary)   |
| bitmap_from_array |      UDF       |                        array转化为bitmap                        |     bitmap_from_array(array)     |  bitmap (binary)   |
|  bitmap_to_array  |      UDF       |                        bitmap转化为array                        |     bitmap_to_array(bitmap)      |     array<int>     |
|  bitmap_contains  |      UDF       |                   bitmap是否包含另一个bitmap全部元素                    | bitmap_contains(bitmap1,bitmap2) |      boolean       |
|  bitmap_contains  |      UDF       |                        bitmap是否包含某个元素                        |   bitmap_contains(bitmap,num)    |      boolean       |
| bitmap_intersect  |   **UDAF**     |                         多个bitmap的交集                          |     bitmap_intersect(bitmap)     |       bitmap (binary)      |
| bitmap_to_string  |      UDF       |     bitmap转化为String        |       bitmap_to_string(bitmap)   |             string               |
## Example
Function name: udfstudio_ + UDF/UDAF name

`to_bitmap -> udfstudio_to_bitmap`

### UDF/UDAF Test
``` hiveql
create table mammut_user.dws_bitmap (
    id string,
    device_id_btm binary
);
create table mammut_user.dwd_data (
    id string,
    device_id int
);
create table mammut_user.array2bitmapTest (
    id int,
    arr array<int>
);
-- insert data
insert overwrite table mammut_user.array2bitmapTest
select 1 as id, array(1, 2, 3, 4) as arr 
union all 
select 2 as id, array(2, 3, 6, 8) as arr
-------
insert overwrite table mammut_user.dwd_data
select 'a' as id, 1001 as device_id
union all 
select 'a' as id, 1001 as device_id
union all 
select 'b' as id, 1002 as device_id
union all
select 'c' as id, 1003 as device_id
union all 
select 'd' as id, 1004 as device_id
union all 
select 'b' as id, 1006 as device_id
union all 
select 'e' as id, 1005 as device_id
union all 
select 'f' as id, 1006 as device_id
union all 
select 'g' as id, 1007 as device_id
union all 
select 'e' as id, 1002 as device_id  
union all 
select 'f' as id, 1001 as device_id  
union all 
select 'g' as id, 1003 as device_id  
union all 
select 'a' as id, 1008 as device_id
union all 
select 'b' as id, 1009 as device_id
union all 
select 'c' as id, 1010 as device_id
union all 
select 'd' as id, 1011 as device_id
union all 
select 'e' as id, 1012 as device_id
union all 
select 'f' as id, 1013 as device_id
union all 
select 'g' as id, 1014 as device_id;
-- input tmp data 
insert into table mammut_user.dws_bitmap select 'k' as id, udfstudio_to_bitmap(1001) as device_id_btm;
-- check data
select * from mammut_user.dwd_data
select * from mammut_user.array2bitmapTest
select id, udfstudio_bitmap2array(device_id_btm) from mammut_user.dws_bitmap

-- generate bitmap
insert overwrite table mammut_user.dws_bitmap
select id, udfstudio_to_bitmap(device_id) as device_id
from mammut_user.dwd_data 
group by id

-- check bitmap2array
select id, udfstudio_bitmap2array(device_id_btm) as arr
from mammut_user.dws_bitmap

-- contains&and&or&xor&count
select 
    udfstudio_bitmap2array(device_id_btm1)                                        as btm1            
    ,udfstudio_bitmap2array(device_id_btm2)                                       as btm2           
    ,udfstudio_bitmap_count(device_id_btm1)                                       as btm1_count      -- bitmap size 
    ,udfstudio_bitmap_contains_bitmap(device_id_btm1, device_id_btm2)             as is_contains     -- contains bitmap            
    ,udfstudio_bitmap_contains_num(device_id_btm1, 1013)                          as is_contains_num -- contains num               
    ,udfstudio_bitmap2array(udfstudio_bitmap_and(device_id_btm1, device_id_btm2)) as btms_and        -- bitmap1 and bitmap2       
    ,udfstudio_bitmap2array(udfstudio_bitmap_or(device_id_btm1, device_id_btm2))  as btms_or         -- bitmap1 or bitmap2       
    ,udfstudio_bitmap2array(udfstudio_bitmap_xor(device_id_btm1, device_id_btm2)) as btms_xor        -- bitmap1 xor bitmap2       
from 
(
    select id1, device_id_btm1, device_id_btm2
    from (
        select id as id1, device_id_btm as device_id_btm1
        from mammut_user.dws_bitmap
        where id = 'f'
    ) a 
    left join 
    (
        select id, device_id_btm as device_id_btm2
        from mammut_user.dws_bitmap
        where id = 'k'
    ) b on 1=1
) w 

-- intersect&union
select 
    udfstudio_bitmap2array(udfstudio_bitmap_intersect(device_id_btm))  as intersect_btm  -- intersect
    ,udfstudio_bitmap2array(udfstudio_bitmap_union(device_id_btm))     as union_btm      -- union
from 
(
    select id, device_id_btm
    from mammut_user.dws_bitmap
    where id in ('a', 'f')
    -- Aggregation can also be done here using group by
) a 

-- array2bitmap
select id, udfstudio_bitmap2array(udfstudio_array2bitmap(arr)) as btm
from mammut_user.array2bitmapTest

-- bitmap2string
select id, udfstudio_bitmap_to_string(device_id_btm) as str
from mammut_user.dws_bitmap
where id = 'f'
```
### 「Data deduplication」count(distinct) v.s bitmap
``` hiveql
create table if not exist `hive_table`
( 
    k      int      comment 'id',
    uuid   bigint   comment '用户id'
) comment 'hive 普通类型表' 
store as ORC;

-- 普通查询（计算去重人数）
select count(distinct uuid) from hive_table;

-- bitmap查询（计算去重人数）
select udfstudio_bitmap_count(udfstudio_to_bitmap(uuid)) from hive_table;

```