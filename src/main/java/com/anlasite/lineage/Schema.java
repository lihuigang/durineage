package com.anlasite.lineage;

import com.alibaba.druid.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    private DB owner;
    private String id;
    private String name;
    private List<Table> tables=new ArrayList<>();

    public DB getOwner() {
        return owner;
    }

    public void setOwner(DB owner) {
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

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public boolean putTable(Table table){
        if(StringUtils.isEmpty(table.getName())){
            return false;
        }
        table.setOwner(this);
        for(int i=0;i<tables.size();i++){
            if(table.getName().equals(tables.get(i).getName())){
                tables.set(i,table);
                return true;
            }
        }
        return tables.add(table);
    }

    public boolean delTable(Table table){
        if(StringUtils.isEmpty(table.getName())){
            return false;
        }
        if(tables==null||tables.size()<=0){
            table.setOwner(null);
            return true;
        }
        for(int i=0;i<tables.size();i++){
            if(table.getName().equals(tables.get(i).getName())){
                tables.remove(i);
                table.setOwner(null);
                return true;
            }
        }
        return false;
    }

    public Table getTable(Table table){
        if(StringUtils.isEmpty(table.getName())){
            return null;
        }
        for(int i=0;i<tables.size();i++){
            if(table.getName().equals(tables.get(i).getName())){
                return tables.get(i);
            }
        }
        if(tables.add(table)){
            table.setOwner(this);
            return table;
        }
        return null;
    }
}
