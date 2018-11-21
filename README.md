# Fluentable

---

> 基于原生的 TableStore SDK 提供的 Fluent API 和 ORM 框架。

## 示例

TableStore 有一个`student`表，主键依次为：`school`、`grade`和`name`。引入 Fluentable 的依赖，对于 Java 类表示为：

    @Getter
    @Setter
    public class Student {

        @TableStorePrimaryKey(type = PrimaryKeyType.INTEGER, order = 2)
        private Integer grade;
        @TableStorePrimaryKey(order = 1)
        private String school;
        @TableStorePrimaryKey(order = 3)
        private String name;

        /**
         * 性别
         */
        private boolean sex;
        /**
         * 爱好
         */
        private String hobby;

        @TableStoreColumn(type = ColumnType.INTEGER)
        private Date birth;

        /**
         * 身高
         */
        private float height;

        @Override
        public String toString() {
            return "Student{" +
                "grade=" + grade +
                ", school='" + school + '\'' +
                ", name='" + name + '\'' +
                ", sex=" + sex +
                ", hobby='" + hobby + '\'' +
                ", birth=" + birth +
                ", height=" + height +
                '}';
        }
    }

添加配置`@Import(FluentableAutoConfiguration.class)`，并注入`TableStoreOperations`和`TableStoreAnnotationParser`。

    @Autowired
    private SyncClientInterface syncClient;
    @Autowired
    private TableStoreOperations tableStoreTemplate;
    @Autowired
    private TableStoreAnnotationParser annotationParser;

    @Test
    public void select() {
        SingleRowQueryCriteria rowQuery = tableStoreTemplate.select().from("student").where()
            .pkEqual(new TableStorePkBuilder()
                .add("school", "铃兰男子高中")
                .add("grade", 3)
                .add("name", "泷谷源治")
                .build()).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(rowQuery));
        Student student = annotationParser.parse(response.getRow(), Student.class).getObject();
        System.out.println(student);
    }

    @Test
    public void insert() {
        Student student = new Student();
        student.setSchool("铃兰男子高中");
        student.setGrade(3);
        student.setName("泷谷源治");
        student.setSex(true);
        student.setHobby("制霸铃兰");
        student.setBirth(new Date());
        student.setHeight(1.84f);

        RowPutChange rowChange = tableStoreTemplate.put("student")
            .with(annotationParser.parse(student))
            .rowNotExist().rowChange();
        syncClient.putRow(new PutRowRequest(rowChange));
    }

    @Test
    public void update() {
        SingleRowQueryCriteria rowQuery = tableStoreTemplate.select().from("student").where()
            .pkEqual(new TableStorePkBuilder()
                .add("school", "铃兰男子高中")
                .add("grade", 3)
                .add("name", "泷谷源治")
                .build()).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(rowQuery));
        Student student = annotationParser.parse(response.getRow(), Student.class).getObject();

        student.setHobby(null); // 默认null将删除 TableStore 的 hobby 字段
        student.setHeight(1.90f);
        RowUpdateChange rowChange = tableStoreTemplate.update("student")
            .with(annotationParser.parse(student))
            .rowExist().rowChange();
        syncClient.updateRow(new UpdateRowRequest(rowChange));
    }

    @Test
    public void delete() {
        RowDeleteChange rowChange = tableStoreTemplate.delete("student")
            .where(new TableStorePkBuilder()
                .add("school", "铃兰男子高中")
                .add("grade", 3)
                .add("name", "泷谷源治")
                .build()).rowExist().rowChange();
        syncClient.deleteRow(new DeleteRowRequest(rowChange));
    }


## Fluent API

使用表格存储提供的 SDK，代码繁琐且容易出错，首先看一个原生单行读的示例：

    // 构造主键
    PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
    PrimaryKey primaryKey = primaryKeyBuilder.build();
    // 读一行
    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria("table", primaryKey);
    // 设置读取最新版本
    criteria.setMaxVersions(1);
    // 设置过滤器, 当Col0的值为0时返回该行.
    SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
    SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
    // 如果不存在Col0这一列, 也不返回.
    singleColumnValueFilter.setPassIfMissing(false);
    criteria.setFilter(singleColumnValueFilter);

使用 Fluent API：

    tableStoreTemplate.select().from("table").where()
        .pkEqual(new TableStorePkBuilder().add("pk", pkValue).build())
        .filter(eq("Col0", ColumnValue.fromLong(0))).rowQuery();

比较一下就可以看到 Fluent API 方式的代码简洁度和可读性都更高，下面对 API 进行详细说明：

    public interface TableStoreOperations {
    
        /**
         * 读取该行所有的列，支持单行读、范围（迭代）读和批量读。默认读取最新版本，范围读默认限制每次返回100条
         *
         * @return
         */
        TableStoreSelect select();
    
        /**
         * 读取行的指定列，支持单行读、范围（迭代）读和批量读。默认读取最新版本，范围读默认限制每次返回100条
         *
         * @param columnNames
         * @return
         */
        TableStoreSelect select(String... columnNames);
    
        /**
         * RowPutChange
         *
         * @param tableName
         * @return
         */
        TableStorePut put(String tableName);
    
        /**
         * RowUpdateChange
         *
         * @param tableName
         * @return
         */
        TableStoreUpdate update(String tableName);
    
        /**
         * RowDeleteChange
         *
         * @param tableName
         * @return
         */
        TableStoreDelete delete(String tableName);
    }

### Select
`TableStoreOperations`提供两个重载的`select`方法，唯一的区别在于：无参数查询行的所有列，有参数查询指定的列。通过之前的例子可以发现写法类似于 SQL，由于 TableStore 是通过主键区别单行读、范围（迭代）读和批量读，所以在`where`方法后需要指定：

                                   / pkEqual 单行读
    select() -> from(表) -> where() - pkIn 批量读
                                   \ pkBetween 范围读

无论单行读、批量读还是范围读，都可以通过`filter`方法设置过滤器（推荐使用`TableStoreFilters`），通过`maxVersions`方法设置要返回列的版本的个数。对于范围读，还可以通过`maxCount`方法设置`Iterator`查询返回的最大行数（默认所有），通过`bufferSize`方法设置每次请求返回的最大行数（默认`100`）,通过`orderBy`方法指定排序（默认正序）。

### Put
插入一行数据，先看下原生的写法：

    // 构造主键
    PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
    PrimaryKey primaryKey = primaryKeyBuilder.build();
    RowPutChange rowPutChange = new RowPutChange("table", primaryKey);
    // 期望原行存在 , 且Col0的值大于100时写入
    Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
    condition.setColumnCondition(new SingleColumnValueCondition("Col0",
    SingleColumnValueCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
    rowPutChange.setCondition(condition);
    //加入一些属性列
    long ts = System.currentTimeMillis();
    rowPutChange.addColumn(new Column("Col0", ColumnValue.fromLong(0), ts));
    rowPutChange.addColumn(new Column("Col0", ColumnValue.fromLong(1), ts + 1));
    rowPutChange.addColumn(new Column("Col1", ColumnValue.fromLong(1));

使用 Fluent API：

    tableStoreTemplate
        .put("table")
        .add(new Column("Col0", ColumnValue.fromLong(0), ts))
        .add(new Column("Col0", ColumnValue.fromLong(1), ts + 1))
        .add(new Column("Col1", ColumnValue.fromLong(1)))
        .where(new TableStorePkBuilder().add("pk", pkValue).build())
        .rowExist().condition(gt("Col0", ColumnValue.fromLong(100))).rowChange();

先指定要操作的表，然后写入属性列。因为 TableStore 的 PutRow 只操作单行，所以需在`where`方法中指定行的主键。

                                           / rowNotExist() 期望该行不存在
    put(表) -> add(属性列)[..] -> where(主键) - rowExist() 期望该行存在
                                           \ rowIgnore() 不对行是否存在做任何判断

通过`condition`方法设置条件（推荐使用`TableStoreConditions`）。

### Update
更新一行数据，先看下原生的写法：

    // 构造主键
    PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
    PrimaryKey primaryKey = primaryKeyBuilder.build();
    RowUpdateChange rowUpdateChange = new RowUpdateChange("table", primaryKey);
    // 期望原行存在, 且Col0的值大于100时更新
    Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
    condition.setColumnCondition(new SingleColumnValueCondition("Col0",
    SingleColumnValueCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
    rowUpdateChange.setCondition(condition);
    // 更新一些列
    rowUpdateChange.put(new Column("Col1", ColumnValue.fromLong(1)));
    rowUpdateChange.put(new Column("Col2", ColumnValue.fromLong(2)));
    // 删除某列的某一版本
    rowUpdateChange.deleteColumn("Col10", 1465373223000L);
    // 删除某一列
    rowUpdateChange.deleteColumns("Col11");

使用 Fluent API：

    tableStoreTemplate.update("table")
        .put(new Column("Col1", ColumnValue.fromLong(1)))
        .put(new Column("Col2", ColumnValue.fromLong(2)))
        .delete("Col10", 1465373223000L)
        .delete("Col11")
        .where(new TableStorePkBuilder().add("pk", pkValue).build())
        .rowIgnore().condition(gt("Col0", ColumnValue.fromLong(100))).rowChange();

update 操作写法类似于 put 操作，即可以指定要写入的属性列，还可以指定要删除的属性列。

                                                             / rowNotExist() 期望该行不存在
    put(表) -> put(属性列)[..]/delete(属性列)[..] -> where(主键) - rowExist() 期望该行存在
                                                             \ rowIgnore() 不对行是否存在做任何判断

也是通过`condition`方法设置条件。

### Delete
删除一行数据，先看下原生的写法：

    // 构造主键
    PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
    PrimaryKey primaryKey = primaryKeyBuilder.build();
    RowDeleteChange rowDeleteChange = new RowDeleteChange("table", primaryKey);
    // 期望原行存在, 且Col0的值大于100时删除
    Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
    condition.setColumnCondition(new SingleColumnValueCondition("Col0",
    SingleColumnValueCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
    rowDeleteChange.setCondition(condition);

使用 Fluent API：

    tableStoreTemplate.delete("table")
        .where(new TableStorePkBuilder().add("pk", pkValue).build())
        .rowExist().condition(gt("Col0", ColumnValue.fromLong(100))).rowChange();

写法类似 SQL，因为 TableStore 只操作单行，所以需在`where`方法中指定行的主键。

                            / rowNotExist() 期望该行不存在
    delete(表) -> where(主键) - rowExist() 期望该行存在
                            \ rowIgnore() 不对行是否存在做任何判断

也是通过`condition`方法设置条件。

### 可选的列值

TableStore SDK 提供的`ColumnValue`不允许值为`null`，Fluentable 提供更宽松的列值`OptionalColumnValue`，允许值为`null`。

 - 对于`TableStorePut`执行`add`添加`OptionalColumnValue`，如果值为null则不保存。
 - 对于`TableStoreUpdate`执行`set`添加`OptionalColumnValue`，如果值为null则删除列，否则更新列。

## ORM框架

> 简化了 TableStore Row 到 Object 互相转换的复杂过程

先看个示例，假设要查询 TableStore 的`person`表，我们有个 Class 与之对应：

    @Getter
    @Setter
    public class Person {
    
        private String id;
        private String name;
        private int age;
        private boolean sex;
    }

当我们查询`person`表时返回`Row`，需要组装`Person`。

    Person person = new Person();
    String id = row.getPrimaryKey().getPrimaryKeyColumn("id").getValue().asString();
    person.setId(id);
    Column nameColumn = row.getLatestColumn("name");
    if (nameColumn != null) {
        person.setName(nameColumn.getValue().asString());
    }
    Column ageColumn = row.getLatestColumn("age");
    if (ageColumn != null) {
        person.setAge((int)ageColumn.getValue().asLong());
    }
    Column sexColumn = row.getLatestColumn("sex");
    if (sexColumn != null) {
        person.setSex(sexColumn.getValue().asBoolean());
    }

可以看到即使简单的`Person`只有4个属性，仍需要写大量繁琐的代码来组装。ORM 框架提供了`@TableStorePrimaryKey`注解标识 TableStore 主键，如下：

    @Getter
    @Setter
    public class Person {
    
        @TableStorePrimaryKey
        private String id;
        private String name;
        private int age;
        private boolean sex;
    }

使用`TableStoreAnnotationParser`解析器即可获取组装的对象。

    @Autowired
    private TableStoreAnnotationParser annotationParser;
    
    // method
    Person person = annotationParser.parse(row, Person.class).getObject();

只需要一行代码搞定！

### 主键注解和列注解
`@TableStorePrimaryKey`标识字段作为 TableStore 表中的主键（不可省略），`@TableStoreColumn`标识字段作为 TableStore 表中的列（可省略）。不指定列名时，默认将字段名作为列名。`@TableStorePrimaryKey`注解默认列类型为`STRING`，可以通过`type`属性来设置主键类型。如果省略在字段上标识`@TableStoreColumn`注解，会根据字段的类型进行推导列类型，以下为内置的类型推导：

|字段类型（Java）|列类型（TableStore）|
|---|---|
|String|STRING|
|Long、long、Integer、int|INTEGER|
|Boolean、boolean|BOOLEAN|
|Double、double、Float、float|DOUBLE|
|Byte[]、byte[]|BINARY|

如果字段定义的类型不在上表中，需要使用`@TableStoreColumn`注解的`type`属性来设置列类型（默认`STRING`）。

### 类型转换
#### Row 转 Object

> 查询 TableStore 返回 Row，解析为 Object

如果字段定义的类型为复杂类型，对于 TableStore 的列类型为`STRING`，此时需要实现`TableStoreConverter`接口，可声明为 Spring 的 Bean，在注解的`converter`属性中指定转换器的类。

    @FunctionalInterface
    public interface TableStoreConverter {

        /**
         * convert TableStore column value to Java field value
         *
         * @param columnEnum
         * @param columnValue
         * @param fieldType
         * @return field value
         */
        Object convert(TableStoreType columnEnum, Object columnValue, Class<?> fieldType);
    }

下表为框架内置的类型转换：

|列类型（TableStore）|字段类型（Java）|
|---|---|
|STRING|String、Integer、int、Long、long、BigDecimal|
|INTEGER|Long、long、Integer、int、String、BigDecimal、Date|
|BOOLEAN|Boolean、boolean、String|
|DOUBLE|Double、double、Float、float、String、BigDecimal|
|BINARY|Byte[]、byte[]、String|

#### Object 转 Row

> 插入、更新 Object，解析为 Row

如果字段定义的类型为复杂类型，对于 TableStore 的列类型为`STRING`，想序列化对象为 Json。此时需要实现`TableStoreFormatter`接口，可声明为 Spring 的 Bean，在注解的`formatter`属性中指定格式化器的类。

    @FunctionalInterface
    public interface TableStoreFormatter {

        /**
         * format Java field value to TableStore column value
         *
         * @param columnEnum
         * @param fieldValue
         * @param fieldType
         * @return column value, Null is not allowed.
         */
        OptionalColumnValue format(TableStoreType columnEnum, Object fieldValue, Class<?> fieldType);
    }

下表为框架内置的类型格式化：

|字段类型（Java）|列类型（TableStore）|
|---|---|
|All|STRING|
|Long、long、Integer、int、String、Date|INTEGER|
|Boolean、boolean、String|BOOLEAN|
|Double、double、Float、float、String、BigDecimal|DOUBLE|
|Byte[]、byte[]、String|BINARY|

## RouteMap

1. Provide English version of README: README.md
2. Update Chinese version of README: README-cn.md
3. Replace Chinese comments and javadoc in code files with English.
4. Add more Fluent APIs, expect cover 50% for year 2018, and more than 90% for year 2019.
5. Add Unit tests, expect code coverage 50% for year 2018, and more than 90% for year 2019.

