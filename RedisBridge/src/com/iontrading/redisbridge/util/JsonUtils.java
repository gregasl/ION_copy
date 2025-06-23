package com.iontrading.redisbridge.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides JSON utilities.
 * This class is responsible for JSON utilities.
 */
public class JsonUtils {
    
    /**
     * Converts an object to a JSON string.
     * 
     * @param object The object to convert
     * @return The JSON string
     */
    public static String toJson(Object object) {
        if (object == null) {
            return "null";
        }
        
        if (object instanceof Map) {
            return mapToJson((Map<?, ?>) object);
        } else if (object instanceof List) {
            return listToJson((List<?>) object);
        } else if (object instanceof String) {
            return "\"" + escapeString((String) object) + "\"";
        } else if (object instanceof Number || object instanceof Boolean) {
            return object.toString();
        } else {
            // For other objects, convert to string
            return "\"" + escapeString(object.toString()) + "\"";
        }
    }
    
    /**
     * Converts a map to a JSON string.
     * 
     * @param map The map to convert
     * @return The JSON string
     */
    private static String mapToJson(Map<?, ?> map) {
        if (map == null) {
            return "null";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            
            sb.append("\"").append(escapeString(entry.getKey().toString())).append("\":");
            sb.append(toJson(entry.getValue()));
        }
        
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * Converts a list to a JSON string.
     * 
     * @param list The list to convert
     * @return The JSON string
     */
    private static String listToJson(List<?> list) {
        if (list == null) {
            return "null";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        
        boolean first = true;
        for (Object item : list) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            
            sb.append(toJson(item));
        }
        
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Escapes a string for JSON.
     * 
     * @param str The string to escape
     * @return The escaped string
     */
    private static String escapeString(String str) {
        if (str == null) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(c);
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Converts a JSON string to a map.
     * 
     * @param json The JSON string
     * @return The map
     */
    public static Map<String, Object> fromJson(String json) {
        // This is a simplified implementation
        // In a real application, you would use a proper JSON parser
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Basic parsing for simple JSON objects
            if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
                return result;
            }
            
            // Remove curly braces
            String content = json.trim().substring(1, json.trim().length() - 1);
            
            // Split by commas, but not commas inside quotes
            String[] pairs = content.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            
            for (String pair : pairs) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    
                    // Remove quotes from key
                    if (key.startsWith("\"") && key.endsWith("\"")) {
                        key = key.substring(1, key.length() - 1);
                    }
                    
                    // Parse value
                    Object parsedValue = parseValue(value);
                    result.put(key, parsedValue);
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Parses a JSON value.
     * 
     * @param value The JSON value
     * @return The parsed value
     */
    private static Object parseValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        
        value = value.trim();
        
        if (value.equals("null")) {
            return null;
        } else if (value.equals("true")) {
            return Boolean.TRUE;
        } else if (value.equals("false")) {
            return Boolean.FALSE;
        } else if (value.startsWith("\"") && value.endsWith("\"")) {
            // String value
            return value.substring(1, value.length() - 1);
        } else if (value.startsWith("{") && value.endsWith("}")) {
            // Object value
            return fromJson(value);
        } else if (value.startsWith("[") && value.endsWith("]")) {
            // Array value
            return parseArray(value);
        } else {
            // Number value
            try {
                if (value.contains(".")) {
                    return Double.parseDouble(value);
                } else {
                    return Long.parseLong(value);
                }
            } catch (NumberFormatException e) {
                return value;
            }
        }
    }
    
    /**
     * Parses a JSON array.
     * 
     * @param array The JSON array
     * @return The parsed array
     */
    private static List<Object> parseArray(String array) {
        List<Object> result = new ArrayList<>();
        
        try {
            // Remove square brackets
            String content = array.trim().substring(1, array.trim().length() - 1);
            
            if (content.trim().isEmpty()) {
                return result;
            }
            
            // Split by commas, but not commas inside quotes or objects/arrays
            String[] items = content.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)(?![^\\[]*\\])(?![^{]*})");
            
            for (String item : items) {
                Object parsedItem = parseValue(item.trim());
                result.add(parsedItem);
            }
        } catch (Exception e) {
            System.err.println("Error parsing JSON array: " + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Private constructor to prevent instantiation.
     */
    private JsonUtils() {
        // Do nothing
    }
}
