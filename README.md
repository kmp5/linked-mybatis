<h2>Linked-Mybatis</h2> 
是一个 MyBatis-Plus 的增强工具，为实现 No XML, No Mapper 而生。</br>
它可以通过函数式编程加Lambda表达式实现连表查询，支持having、groupby、exists等语句，支持排序、分页、调用存储过程等常见开发场景。</br>
它的实现原理很简单，通过函数式编程拼接Sql，然后根据Sql及查询参数创建PreparedStatement进行查询。</br>
由于没有Mapper代码，要实现Mybatis的一二级缓存比较困难，Linked-Mybatis提供了另一种基于Redis的分布式缓存方案。</br>
目前仅支持Mysql</br></br>
作者：kzow3n，QQ邮箱：442764882@qq.com</br>
API站点：http://ivublazor.top:8083/</br>
Maven仓库地址：https://mvnrepository.com/artifact/io.github.kmp5/sqlrunner-plus</br>

