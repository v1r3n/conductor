package com.netflix.conductor.sdk.workflow.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class Utils {

    public static Map<String, Object> toInputMap(String prefix, Class<?> clazz) {
        Map<String, Object> map = new HashMap<>();
        append(map, prefix + ".output", clazz);
        return map;
    }

    private static void append(Map<String, Object> map, String prefix, Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            if(field.getType().isPrimitive() || field.getType().equals(String.class)) {
                map.put(name, "${" + prefix + "." + field.getName() + "}");
            } else if(field.getType().getPackageName().startsWith("java") || field.getType().getPackageName().startsWith("jdk")){
                //skip
            } else if (field.getType().isEnum()) {
                map.put(name, "${" + prefix + "." + field.getName() + "}");
            }else {
                Map<String, Object> subMap = new HashMap<>();
                map.put(name, subMap);
                append(subMap, prefix + "." + name, field.getType());
            }
        }
    }
}
