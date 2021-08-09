<h2>Jdbc-Plus</h2> 
是一个 MyBatis-Plus 的增强工具，为进一步简化开发、提高效率而生。</br>
它可以通过函数式编程加Lambda表达式实现连表查询，同样支持having、groupby、exist等语句，支持排序、分页查询、调用存储过程等常见开发场景，满足了我开发期间95%以上的复杂Sql。</br>
它的实现原理很简单，通过函数式编程拼接想要的Sql，然后传递Sql及查询参数给JdbcTemplate进行查询。</br>
目前仅支持Mysql</br>
作者：kzow3n，QQ邮箱：442764882@qq.com</br>
API站点：http://ivublazor.top:8083/
Maven仓库地址：https://mvnrepository.com/artifact/io.github.kmp5/jdbc-plus/0.0.1

基本连表查询示例：
SqlWrapper sqlWrapper = new SqlWrapper();
List<Student> students = sqlWrapper
	.selectAll(1)
	.from(Student.class, "s")
	.InnerJoin(Teacher.class, "t")
	.on(1, Student::getHeadmasterId, 2, Teacher::getId)
	.eq(2, Teacher::getName, "李老师")
	.formatSql()
	.queryForObjects(Student.class, jdbcTemplate);
