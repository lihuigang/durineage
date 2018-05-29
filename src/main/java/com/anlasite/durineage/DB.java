package com.anlasite.durineage;

import java.util.ArrayList;
import java.util.List;

public class DB {

    private List<Schema> schemas=new ArrayList<>();

    public List<Schema> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<Schema> schemas) {
        this.schemas = schemas;
    }

    public boolean putSchema(Schema schema){
        schema.setOwner(this);
        for(int i = 0; i< schemas.size(); i++){
            if(schema.getName()==null){
                if(schemas.get(i).getName()==null){
                    schemas.set(i,schema);
                    return true;
                }
            }else if(schema.getName().equals(schemas.get(i).getName())){
                schemas.set(i,schema);
                return true;
            }
        }
        schemas.add(schema);
        return true;
    }

    public boolean delSchema(Schema schema){
        if(schemas ==null|| schemas.size()<=0){
            schema.setOwner(null);
            return true;
        }
        for(int i = 0; i< schemas.size(); i++){
            if(schema.getName()==null){
                if(schemas.get(i).getName()==null){
                    schemas.remove(i);
                    schema.setOwner(null);
                    return true;
                }
            }else if(schema.getName().equals(schemas.get(i).getName())){
                schemas.remove(i);
                schema.setOwner(null);
                return true;
            }
        }
        return false;
    }

    /**
     * 如果schema已存在就返回已存在的schema；如果不存在，就插入所要查询的schema，并且返回。可以理解为“合并schema”
     * @param schema
     * @return
     */
    public Schema getSchema(Schema schema){
        for(int i = 0; i< schemas.size(); i++){
            if(schema.getName()==null){
                if(schemas.get(i).getName()==null){
                    return schemas.get(i);
                }
            }else if(schema.getName().equals(schemas.get(i).getName())){
                return schemas.get(i);
            }
        }
        if(schemas.add(schema)){
            schema.setOwner(this);
            return schema;
        }
        return null;
    }
}
