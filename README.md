<h2>SqlRunner-Plus</h2> 
是一个 MyBatis-Plus 的增强工具，为进一步简化开发、提高效率而生。</br>
它可以通过函数式编程加Lambda表达式实现连表查询，支持having、groupby、exists等语句，支持排序、分页、调用存储过程等常见开发场景，真正意义上实现No XML, No Mapper。</br>
它的实现原理很简单，通过函数式编程拼接Sql，然后传递Sql及查询参数给ibatis.jdbc.SqlRunner进行查询，因此同样适用一二级缓存。</br>
目前仅支持Mysql</br></br>
作者：kzow3n，QQ邮箱：442764882@qq.com</br>
API站点：http://ivublazor.top:8083/</br>
Maven仓库地址：https://mvnrepository.com/artifact/io.github.kmp5/sqlrunner-plus</br>

