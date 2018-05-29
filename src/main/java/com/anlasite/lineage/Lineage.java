package com.anlasite.lineage;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;

import java.lang.reflect.Field;
import java.util.List;

public class Lineage {

    private DB db;

    public Lineage() {
        db=new DB();
    }

    public Lineage(DB db) {
        this.db = db;
    }

    public DB getDb() {
        return db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    /**
     * 获取血缘关系
     * @param sql
     * @param dbType 数据库类型
     * @return
     */
    public DB getInfo(String sql, String dbType){
        //格式化sql，并过滤不支持关键字
        sql = SQLUtils.format(sql.toLowerCase().replaceAll(" nolock","").replaceAll(" if exists",""), dbType);
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        for (int i = 0; i < stmtList.size(); i++) {
            if(stmtList.get(i).getClass().equals(SQLDropTableStatement.class)){
                //删表语句
                SQLDropTableStatement statement= (SQLDropTableStatement) stmtList.get(i);
                for(SQLExprTableSource tableSource:statement.getTableSources()){
                    Schema schema =new Schema();
                    if(tableSource.getExpr() instanceof SQLPropertyExpr){
                        //获取schema
                        schema.setName(((SQLIdentifierExpr)((SQLPropertyExpr)tableSource.getExpr()).getOwner()).getName());
                    }
                    //合并schema
                    schema=db.getSchema(schema);
                    Table table=new Table();
                    //获取该表
                    if(tableSource.getExpr() instanceof SQLPropertyExpr){
                        table.setName(((SQLPropertyExpr)tableSource.getExpr()).getName());
                    }else{
                        table.setName(((SQLIdentifierExpr)tableSource.getExpr()).getName());
                    }
                    //合并表
                    table=schema.getTable(table);
                    //因为是删表语句，所以肯定说明该表是临时表
                    table.setTemp(true);
                }
            }else if(stmtList.get(i).getClass().equals(SQLCreateTableStatement.class)){
                //建表语句
                SQLCreateTableStatement statement= (SQLCreateTableStatement) stmtList.get(i);
                Schema schema =new Schema();
                if(statement.getTableSource().getExpr() instanceof SQLPropertyExpr){
                    //获取schema
                    schema.setName(((SQLIdentifierExpr)((SQLPropertyExpr)statement.getTableSource().getExpr()).getOwner()).getName());
                }
                schema=db.getSchema(schema);
                Table table=new Table();
                //获取表
                if(statement.getTableSource().getExpr() instanceof SQLPropertyExpr){
                    table.setName(((SQLPropertyExpr)statement.getTableSource().getExpr()).getName());
                }else{
                    table.setName(((SQLIdentifierExpr)statement.getTableSource().getExpr()).getName());
                }
                //合并表
                table=schema.getTable(table);
                if(statement.getTableElementList().size()<=0){
                    queryAnalysis(statement.getSelect().getQuery(),table);
                }
                //去除血缘中的临时表
                for(Column column:table.getColumns()){
                    removeTemp(column);
                }
            }else if(stmtList.get(i).getClass().equals(SQLInsertStatement.class)){
                //插入语句
                SQLInsertStatement statement= (SQLInsertStatement) stmtList.get(i);
                Schema schema =new Schema();
                if(statement.getTableSource().getExpr() instanceof SQLPropertyExpr){
                    schema.setName(((SQLIdentifierExpr)((SQLPropertyExpr)statement.getTableSource().getExpr()).getOwner()).getName());
                }
                schema=db.getSchema(schema);
                Table table=new Table();
                if(statement.getTableSource().getExpr() instanceof SQLPropertyExpr){
                    table.setName(((SQLPropertyExpr)statement.getTableSource().getExpr()).getName());
                }else{
                    table.setName(((SQLIdentifierExpr)statement.getTableSource().getExpr()).getName());
                }
                table=schema.getTable(table);
                if(statement.getColumns().size()<=0){
                    queryAnalysis(statement.getQuery().getQuery(),table);
                }else{
                    for(SQLExpr expr:statement.getColumns()){
                        Column column=new Column();
                        column.setName(((SQLIdentifierExpr)expr).getName());
                        table.getColumn(column);
                    }
                    queryAnalysis(statement.getQuery().getQuery(),table,true);
                }
                for(Column column:table.getColumns()){
                    removeTemp(column);
                }
            }
        }
        return db;
    }

    public DB getInfo(String[] sqls, String dbType){
        StringBuffer sql=new StringBuffer();
        for(String str:sqls){
            str=str.replaceAll(";","")+";";
            sql.append(str);
        }
        return getInfo(sql.toString(),dbType);
    }

    public DB getInfo(List<String> sqls, String dbType){
        StringBuffer sql=new StringBuffer();
        for(String str:sqls){
            str=str.replaceAll(";","")+";";
            sql.append(str);
        }
        return getInfo(sql.toString(),dbType);
    }

    private void queryAnalysis(Object object, Table alias, boolean columnSure){
        if(object instanceof SQLSelectQueryBlock){
            List<SQLSelectItem> selectItems=((SQLSelectQueryBlock)object).getSelectList();
            for(int i=0;i<selectItems.size();i++){
                SQLSelectItem selectItem=selectItems.get(i);
                Column column=new Column();
                if(columnSure){
                    column=alias.getColumns().get(i);
                }else{
                    if(selectItem.getAlias()==null){
                        if(selectItem.getExpr() instanceof SQLIdentifierExpr){
                            column.setName(((SQLIdentifierExpr)selectItem.getExpr()).getName());
                        }else if(selectItem.getExpr() instanceof SQLPropertyExpr){
                            column.setName(((SQLPropertyExpr)selectItem.getExpr()).getName());
                        }
                    }else{
                        column.setName(selectItem.getAlias());
                    }
                    column=alias.getColumn(column);
                }
                Schema tempSchema =new Schema();
                findColumn(selectItem, tempSchema);
                for(Table tempTable: tempSchema.getTables()){
                    tempTable.setTemp(true);
                    for(Column tempColumn:tempTable.getColumns()){
                        column.putSrcColumn(tempColumn);
                    }
                }
            }
            Schema tempSchema =new Schema();
            findColumn(selectItems, tempSchema);
            findTable(((SQLSelectQueryBlock)object).getFrom(), tempSchema);
            List<Column> columns=alias.getColumns();
            for(Table tempTable: tempSchema.getTables()){
                for(Column tempColumn:tempTable.getColumns()){
                    for(int j=0;j<columns.size();j++){
                        List<Column> srcColumns=columns.get(j).getSrcColumns();
                        for(int k=0;k<srcColumns.size();k++){
                            Column srcColumn=srcColumns.get(k);
                            if(tempColumn.getName().equals(srcColumn.getName())&&tempColumn.getOwner().getAlias().equals(srcColumn.getOwner().getName())){
                                srcColumns.set(k,tempColumn);
                                break;
                            }
                        }
                    }
                }
                tempTable.setAlias(null);
            }
        }else if(object instanceof SQLUnionQuery){
            Field[] fields = object.getClass().getDeclaredFields();
            int i=0;
            for (Field field : fields) {
                field.setAccessible(true);
                if(field.getName().equals("parent")||field.getName().equals("serialVersionUID")){
                    continue;
                }
                Object f= null;
                try {
                    f = field.get(object);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                if(f instanceof SQLSelectQueryBlock){
                    if(i>0){
                        columnSure=true;
                    }
                    queryAnalysis(f,alias,columnSure);
                    i++;
                }else if(f instanceof SQLUnionQuery){
                    unionQueryAnalysis(f,alias,true);
                }
            }
        }
    }
    private void queryAnalysis(Object object, Table alias){
        queryAnalysis(object,alias,false);
    }
    private void unionQueryAnalysis(Object object, Table alias, boolean columnSure){
        if(object instanceof SQLSelectQueryBlock){
            queryAnalysis(object,alias,columnSure);
        }else if(object instanceof SQLUnionQuery){
            Field[] fields = object.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                if(field.getName().equals("parent")||field.getName().equals("serialVersionUID")){
                    continue;
                }
                Object f= null;
                try {
                    f = field.get(object);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                if(f instanceof SQLSelectQuery){
                    unionQueryAnalysis(f, alias, true);
                }
            }
        }
    }

    private void findColumn(Object object, Schema schema){
        if(object instanceof SQLExpr || object instanceof SQLSelectItem||object instanceof SQLCaseExpr.Item||object instanceof SQLCaseStatement.Item){
            if(object.getClass().equals(SQLPropertyExpr.class)){
                Table table=new Table();
                table.setTemp(true);
                table.setName(((SQLIdentifierExpr)((SQLPropertyExpr) object).getOwner()).getName());
                table=schema.getTable(table);
                table.setAlias(table.getName());
                Column column=new Column();
                column.setName(((SQLPropertyExpr) object).getName());
                table.getColumn(column);
            }else if(!object.getClass().equals(SQLIdentifierExpr.class)){
                Field[] fields = object.getClass().getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    if(field.getName().equals("parent")||field.getName().equals("serialVersionUID")){
                        continue;
                    }
                    Object f= null;
                    try {
                        f = field.get(object);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    if(f==null||f.equals(String.class)){
                        continue;
                    }
                    findColumn(f, schema);
                }
            }
        }else if(object instanceof List){
            for (Object o:(List)object){
                findColumn(o, schema);
            }
        }
    }

    private void findTable(Object object, Schema schema){
        if(object instanceof SQLTableSource){
            if(object instanceof SQLExprTableSource){
                SQLExprTableSource tableSource=(SQLExprTableSource)object;
                if(tableSource.getAlias()!=null){
                    List<Table> tables=schema.getTables();
                    for(int i=0;i<tables.size();i++){
                        Table table=tables.get(i);
                        if(tableSource.getAlias().equals(table.getName())){
                            if(tableSource.getExpr() instanceof SQLPropertyExpr){
                                table.setName(((SQLPropertyExpr)tableSource.getExpr()).getName());
                            }else{
                                table.setName(((SQLIdentifierExpr)tableSource.getExpr()).getName());
                            }
                            table.setTemp(false);
                            Schema newSchema=new Schema();
                            if(tableSource.getExpr() instanceof SQLPropertyExpr){
                                newSchema.setName(((SQLIdentifierExpr)((SQLPropertyExpr)tableSource.getExpr()).getOwner()).getName());
                            }
                            newSchema=db.getSchema(newSchema);
                            Table tempTable=newSchema.getTable(table);
                            if(!table.equals(tempTable)){
                                table=tempTable.merge(table);
                            }
                            table.setAlias(tableSource.getAlias());
                            tables.set(i, table);
                            break;
                        }
                    }
                }
            }else if (object.getClass().equals(SQLSubqueryTableSource.class)){
                SQLSelectQueryBlock query= (SQLSelectQueryBlock) ((SQLSubqueryTableSource)object).getSelect().getQuery();
                for(Table table: schema.getTables()){
                    if(((SQLSubqueryTableSource)object).getAlias().equals(table.getName())){
                        queryAnalysis(query,table);
                        break;
                    }
                }
            }else if(object.getClass().equals(SQLJoinTableSource.class)){
                Field[] fields = object.getClass().getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    if(field.getName().equals("parent")||field.getName().equals("serialVersionUID")){
                        continue;
                    }
                    Object f= null;
                    try {
                        f = field.get(object);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    if(f instanceof SQLTableSource){
                        findTable(f, schema);
                    }
                }

            }
        }
    }

    private void removeTemp(Column column){
        List<Column> srcColumns=column.getSrcColumns();
        for(int i=0;i<srcColumns.size();i++){
            Column srcColumn=srcColumns.get(i);
            if(srcColumn.getOwner().isTemp()){
                srcColumns.remove(i);
                i--;
                for(Column childSrcColumn:srcColumn.getSrcColumns()){
                    column.getSrcColumn(childSrcColumn);
                }
            }else{
                removeTemp(srcColumn);
            }
        }
    }
}
