package com.anlasite.durineage;

import com.alibaba.druid.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Column {

    private Table owner;
    private String id;
    private String name;
    private String type;
    private List<Column> srcColumns=new ArrayList<>();

    public Table getOwner() {
        return owner;
    }

    public void setOwner(Table owner) {
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Column> getSrcColumns() {
        return srcColumns;
    }

    public void setSrcColumns(List<Column> srcColumns) {
        this.srcColumns = srcColumns;
    }

    public boolean putSrcColumn(Column column){
        if(StringUtils.isEmpty(column.getName())){
            return false;
        }
        for(int i=0;i<srcColumns.size();i++){
            if(column.getName().equals(srcColumns.get(i).getName())){
                if(column.getOwner()==null&&srcColumns.get(i).getOwner()==null){
                    srcColumns.set(i,column);
                    return true;
                }else if(column.getOwner()!=null&&srcColumns.get(i).getOwner()!=null&&column.getOwner().getName().equals(srcColumns.get(i).getOwner().getName())){
                    if(column.getOwner().getOwner()==null&&srcColumns.get(i).getOwner().getOwner()==null){
                        srcColumns.set(i,column);
                        return true;
                    }else if(column.getOwner().getOwner()!=null&&srcColumns.get(i).getOwner().getOwner()!=null&&column.getOwner().getOwner().getName().equals(srcColumns.get(i).getOwner().getOwner().getName())){
                        srcColumns.set(i,column);
                        return true;
                    }
                }
            }
        }
        return srcColumns.add(column);
    }

    public boolean delSrcColumn(Column column){
        if(StringUtils.isEmpty(column.getName())){
            return false;
        }
        if(srcColumns==null||srcColumns.size()<=0){
            return true;
        }
        for(int i=0;i<srcColumns.size();i++){
            if(column.getName().equals(srcColumns.get(i).getName())){
                if(column.getOwner()==null&&srcColumns.get(i).getOwner()==null){
                    srcColumns.remove(i);
                    return true;
                }else if(column.getOwner()!=null&&srcColumns.get(i).getOwner()!=null&&column.getOwner().getName().equals(srcColumns.get(i).getOwner().getName())){
                    if(column.getOwner().getOwner()==null&&srcColumns.get(i).getOwner().getOwner()==null){
                        srcColumns.remove(i);
                        return true;
                    }else if((column.getOwner().getOwner()!=null&&srcColumns.get(i).getOwner().getOwner()!=null)&&(column.getOwner().getOwner().getName().equals(srcColumns.get(i).getOwner().getOwner().getName()))){
                        srcColumns.remove(i);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public Column getSrcColumn(Column column){
        if(StringUtils.isEmpty(column.getName())){
            return null;
        }
        for(int i=0;i<srcColumns.size();i++){
            if(column.getName().equals(srcColumns.get(i).getName())){
                if(column.getOwner()==null&&srcColumns.get(i).getOwner()==null){
                    return srcColumns.get(i);
                }else if(column.getOwner()!=null&&srcColumns.get(i).getOwner()!=null&&column.getOwner().getName().equals(srcColumns.get(i).getOwner().getName())){
                    if(column.getOwner().getOwner()==null&&srcColumns.get(i).getOwner().getOwner()==null){
                        return srcColumns.get(i);
                    }else if(column.getOwner().getOwner()!=null&&srcColumns.get(i).getOwner().getOwner()!=null){
                        if((column.getOwner().getOwner().getName()==null&&srcColumns.get(i).getOwner().getOwner().getName()==null)||(column.getOwner().getOwner().getName().equals(srcColumns.get(i).getOwner().getOwner().getName()))){
                            return srcColumns.get(i);
                        }
                    }
                }
            }
        }
        if(srcColumns.add(column)){
            return column;
        }
        return null;
    }

    public Column merge(Column column){
        for(Column srcColumn:column.getSrcColumns()){
            Column thisSrcColumn=getSrcColumn(srcColumn);
            if(!srcColumn.equals(thisSrcColumn)){
                thisSrcColumn.owner.merge(srcColumn.owner);
            }
        }
        return this;
    }
}
