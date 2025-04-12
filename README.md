# Introduction
Project contains hive UDF/UDAF on BitMap

## UDF/UDAF usage

|        UDF        |             描述              |                案例                |     结果类型      |
|:-----------------:|:---------------------------:|:--------------------------------:|:-------------:|
|     to_bitmap     | 将num（int或bigint） 转化为 bitmap |          to_bitmap(num)          |    bitmap     |
|   bitmap_union    |   多个bitmap合并为一个bitmap（并集）   |       bitmap_union(bitmap)       |    bitmap     |
|   bitmap_count    |      计算bitmap中存储的num个数      |       bitmap_count(bitmap)       |     bigint      |
|    bitmap_and     |        计算两个bitmap交集         |   bitmap_and(bitmap1,bitmap2)    |    bitmap     |
|     bitmap_or     |        计算两个bitmap并集         |    bitmap_or(bitmap1,bitmap2)    |    bitmap     |
|    bitmap_xor     |        计算两个bitmap差集         |   bitmap_xor(bitmap1,bitmap2)    |    bitmap     |
| bitmap_from_array |  array 转化为bitmap         	  |     bitmap_from_array(array)     |    bitmap     |
|  bitmap_to_array  |       bitmap转化为array        |     bitmap_to_array(bitmap)      | array<bigint> |
|  bitmap_contains  |   bitmap是否包含另一个bitmap全部元素   | bitmap_contains(bitmap1,bitmap2) |    boolean    |
|  bitmap_contains  |       bitmap是否包含某个元素        |   bitmap_contains(bitmap,num)    |    boolean    |
| bitmap_intersect  |         多个bitmap的交集         |     bitmap_intersect(bitmap)    |   bitmap  |

## Example
```
CREATE TABLE IF NOT EXISTS `hive_bitmap_table`
( 
    k      int      comment 'id',
    bitmap binary   comment 'bitmap'
) comment 'hive bitmap 类型表' 
STORED AS ORC;

-- 数据写入
insert into table  hive_bitmap_table select  1 as id,to_bitmap(1) as bitmap;
insert into table hive_bitmap_table select  2 as id,to_bitmap(2) as bitmap;

-- 查询

select bitmap_union(bitmap) from hive_bitmap_table;
select bitmap_count(bitmap_union(bitmap)) from hive_bitmap_table;

select bitmap_contains(bitmap,1) from hive_bitmap_table;
select bitmap_contains(bitmap,bitmap_from_array(array(1,2))) from hive_bitmap_table;
```

```
CREATE TABLE IF NOT EXISTS `hive_table`
( 
    k      int      comment 'id',
    uuid   bigint   comment '用户id'
) comment 'hive 普通类型表' 
STORED AS ORC;

-- 普通查询（计算去重人数）

select count(distinct uuid) from hive_table;

-- bitmap查询（计算去重人数）

select bitmap_count(to_bitmap(uuid)) from hive_table;
```