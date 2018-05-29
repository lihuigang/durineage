package com.anlasite.durineage;

import com.alibaba.druid.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Table {

    private Schema owner;
    private String id;
    private String name;
    private List<Column> columns=new ArrayList<>();
    private boolean isTemp=false;
    private String alias;

    public Schema getOwner() {
        return owner;
    }

    public void setOwner(Schema owner) {
        this.owner = owner;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public void setTemp(boolean temp) {
        isTemp = temp;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public boolean putColumn(Column column){
        if(StringUtils.isEmpty(column.getName())){
            return false;
        }
        column.setOwner(this);
        for(int i=0;i<columns.size();i++){
            if(column.getName().equals(columns.get(i).getName())){
                columns.set(i,column);
                return true;
            }
        }
        return columns.add(column);
    }

    public boolean delColumn(Column column){
        if(StringUtils.isEmpty(column.getName())){
            return false;
        }
        if(columns==null||columns.size()<=0){
            column.setOwner(null);
            return true;
        }
        for(int i=0;i<columns.size();i++){
            if(column.getName().equals(columns.get(i).getName())){
                columns.remove(i);
                column.setOwner(null);
                return true;
            }
        }
        return false;
    }

    public Column getColumn(Column column){
        if(StringUtils.isEmpty(column.getName())){
            return null;
        }
        for(int i=0;i<columns.size();i++){
            if(column.getName().equals(columns.get(i).getName())){
                return columns.get(i);
            }
        }
        if(columns.add(column)){
            column.setOwner(this);
            return column;
        }
        return null;
    }

    public Table merge(Table table){
        for(Column column:table.getColumns()){
            Column thisColumn=getColumn(column);
            if(!column.equals(thisColumn)){
                thisColumn.merge(column);
            }
        }
        return this;
    }
}
