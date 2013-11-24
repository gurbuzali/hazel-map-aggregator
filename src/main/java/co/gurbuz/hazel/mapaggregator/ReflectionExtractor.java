package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.query.impl.QueryException;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @ali 23/11/13
 */
public class ReflectionExtractor {

    private static String THIS_ATTRIBUTE_NAME = "this";
    private final String attribute;
    private final String[] possibleMethodNames;
    private final Map<String, Getter> getterCache = new HashMap<String, Getter>();

    public ReflectionExtractor(String attribute) {
        this.attribute = attribute;
        possibleMethodNames = new String[3];
        if (attribute != null && attribute.isEmpty()){
            throw new IllegalArgumentException("attribute cannot be empty");
        }
        if (attribute != null && !attribute.equals(THIS_ATTRIBUTE_NAME)){
            final String firstChar = String.valueOf(attribute.charAt(0)).toUpperCase();
            possibleMethodNames[0] = "get"+firstChar+attribute.substring(1);
            possibleMethodNames[1] = "is"+firstChar+attribute.substring(1);
            possibleMethodNames[2] = attribute;
        }

    }

    public <T> T extract(Object value) {
        try {
            if (attribute == null || attribute.equals(THIS_ATTRIBUTE_NAME)) {
                return (T)value;
            }
            final Class<?> _class = value.getClass();
            String cacheKey = _class.getName()+":"+attribute;
            Getter getter = getterCache.get(cacheKey);
            if (getter == null) {
                getter = createGetter(_class);
                getterCache.put(cacheKey, getter);
            }
            if (getter != null) {
                return (T)getter.get(value);
            }
            throw new NoSuchMethodException(attribute);
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    private Getter createGetter(Class _class){
        try {
            final Field field = _class.getDeclaredField(attribute);
            return new Getter(field);
        } catch (NoSuchFieldException ignored1) {
            try {
                final Field field = _class.getField(attribute);
                return new Getter(field);
            } catch (NoSuchFieldException ignored2) {
                for (String methodName : possibleMethodNames) {
                    try {
                        final Method method = _class.getDeclaredMethod(methodName);
                        return new Getter(method);
                    } catch (NoSuchMethodException ignored3) {
                        try {
                            final Method method = _class.getMethod(methodName);
                            return new Getter(method);
                        } catch (NoSuchMethodException ignored4) {
                        }
                    }
                }
            }



        }
        throw new IllegalArgumentException("No suitable field or method found for '"+attribute+"' in '" + _class+"'");
    }

    class Getter {

        Field field;

        Method method;

        Getter(Field field) {
            this.field = field;
        }

        Getter(Method method) {
            this.method = method;
        }

        public Object get(Object obj){
            try {
                if (field != null){
                    return field.get(obj);
                }
                return method.invoke(obj);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }

    }

}
