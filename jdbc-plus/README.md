<h2>Linked-Mybatis</h2> 

- Linked-Mybatis（下面简称LM）是一个 MyBatis-Plus（下面简称MP）的增强工具，为实现 No XML, No Mapper 而生
- LM可以通过函数式编程加Lambda表达式实现连表查询
- LM提供了基于Redis的分布式缓存方案
- 目前支持数据库：Mysql、SqlServer、PgSql、达梦
- 作者：kzow3n，QQ邮箱：442764882@qq.com
- 文档站点：http://159.75.248.176:8082/
- Maven仓库地址：https://mvnrepository.com/artifact/io.github.kmp5/linked-mybatis

<h2>快速上手</h2> 

- 引入jar包
```xml
    <dependency>
        <groupId>io.github.kmp5</groupId>
        <artifactId>linked-mybatis</artifactId>
        <version>linked-mybatis.version</version>
    </dependency>
  ```
  
- 注入SqlSessionFactory、SqlSession
```java
    @Autowired
    protected SqlSessionFactory sqlSessionFactory;
    @Autowired
    protected SqlSession sqlSession;
  ```

<h2>代码示例</h2>
- 基本连表查询
```java
    LinkedQueryWrapper queryWrapper = new LinkedQueryWrapper()
        .selectAll(1)
        .select(2, Teacher::getName, StudentVo::getHeadmasterName)
        .from(Student.class, "s")
        .InnerJoin(Teacher.class, "t")
        .on(1, Student::getHeadmasterId, 2, Teacher::getId)
        .eq(1, Student::getName, "李华")
        ;
    LinkedQueryExecutor queryExecutor = new LinkedQueryExecutorBuilder(sqlSessionFactory, sqlSession).build();
    StudentVo studentVo = queryExecutor.forObject(StudentVo.class, queryWrapper);
```
  
- 分页查询
```java
    LinkedQueryWrapper queryWrapper = new LinkedQueryWrapper()
        .selectAll(1)
        .from(Student.class, "s")
        .InnerJoin(Teacher.class, "t")
        .on(1, Student::getHeadmasterId, 2, Teacher::getId)
        .eq(2, Teacher::getName, "李老师")
        ;
    LinkedQueryExecutor queryExecutor = new LinkedQueryExecutorBuilder(sqlSessionFactory, sqlSession).build();
    Page<Student> page = queryExecutor.forObjectPage(Student.class, queryWrapper, pageIndex, pageSize);
```

- 嵌套查询
```java
    LinkedQueryWrapper queryWrapper = new LinkedQueryWrapper()
        .selectAll(1)
        .from(Student.class, "s")
        .innerJoin(t -> t.from(Score.class, "sc")
            .eq(1, Score::getNo, "001")
            .groupBy(1, Score::getStudentId)
            .having(t -> t.lt(p -> p.avg(1, Score::getScore), 60))
            , "t"
        )
        .on(1, Student::getId, 2, Score::getStudentId)
        ;
    LinkedQueryExecutor queryExecutor = new LinkedQueryExecutorBuilder(sqlSessionFactory, sqlSession).build();
    List<Student> students = queryExecutor.forObjects(Student.class, queryWrapper);
```

- 复杂条件and/or
```java
    LinkedQueryWrapper queryWrapper = new LinkedQueryWrapper()
        .selectAll(1)
        .from(Student.class, "s")
        .innerJoin(Score.class, "sc")
        .on(1, Student::getId, 2, Score::getStudentId)
        .eq(2, Score::getNo, "001")
        .eq(2, Score::getType, "math")
        .and(q -> q.lt(2, Score::getScore, 60)
            .or()
            .ge(2, Score::getScore, 90)
        )
        ;
    LinkedQueryExecutor queryExecutor = new LinkedQueryExecutorBuilder(sqlSessionFactory, sqlSession).build();
    List<Student> students = queryExecutor.forObjects(Student.class, queryWrapper);
```

- 更多示例请浏览：http://159.75.248.176:8082/