# durineage
## 描述
解析SQL语句，分析血缘。不是table-table的关系，是column-column关系。
## 使用
只有一个功能

	Lineage lineage = new Lineage();
	DB db = lineage.getInfo(sql语句, 数据库类型);	//sql语句可为String、String[]、List<String>
## 规范（局限）
### 全局
* 绝对不要用“<font color=red>*</font>”
* 如果字段不参与血缘，尽量不要被“<font color=red>count</font>”，尽量用“<font color=red>count(1)</font>”
* 所有字段格式：<font color=red>别名.字段名</font>
### select
* 绝对不要嵌“<font color=red>子查询</font>”
### from
* 所有表格式：<font color=red>schema.表名</font> [as] <font color=red>别名</font>  
	* 所有表都得有“<font color=red>别名</font>”  
	* 有<font color=red>schema</font>的一定要加<font color=red>schema</font>
* 子查询即使是内外嵌套，<font color=red>别名</font>最好也<font color=red>不要相同</font>
* 关联查询一定要用“<font color=red>join</font>”，不能用其他
### create
* create table schema.表名 <font color=red>as</font> select …  
	* <font color=red>as</font>必须得加
### insert
* insert into schema.表名 <font color=red>(字段名[,…])</font> select …  
	* select前的字段<font color=red>必须得加</font>
### 注
* 默认<font color=red>drop</font>语句表示该表是临时表，最终血缘中<font color=red>不予以展示</font>
* 本工具解析sql的读取功能使用<font color=red>druid</font>实现，所以解析血缘的前提是sql能被druid读取，druid读取出错，就别提下一步解析了。
* 事实上<font color=red>create规范中as必须加</font>中也是druid的要求。
* 实际上druid还<font color=red>不支持</font>“<font color=red>nolock</font>”、“<font color=red>if exists</font>”，只是本工具已经在druid读取之前已经将这两个过滤掉了。目前还有哪些不支持还未知。
