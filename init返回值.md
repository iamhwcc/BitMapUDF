#### Java类型: `int` Hive类型: `int`
```java
return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
```
#### Java类型: `long` Hive类型: `bigint`
```java
return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
```
#### Java类型: `byte[]` Hive类型: `binary`
```java
return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
```
#### Java类型: `String` Hive类型: `string`
```java
return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
```
#### Java类型: `List<Integer>` Hive类型: `array<int>`
```java
return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
```
#### Java类型: `List<Long>` Hive类型: `array<bigint>`
```java
return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
```
#### Java类型: `boolean` Hive类型: `boolean`
```java
return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
```