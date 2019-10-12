package com.myboot.dataprocess.tools;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 配置文件操作
 *
 * @author rootwang
 *
 */

public class ConfigTool {

    private Properties props = new Properties();

    public ConfigTool(String file) {
        String path = "/" + file;
        InputStreamReader is = null;
        try {
            is = new InputStreamReader(ConfigTool.class.getResourceAsStream(path), "UTF-8");
            props.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Properties getProperties() {
        return props;
    }

    public int getInt(String key,int def) {
        if("".equals(props.getProperty(key))){
            return def;
        }else{
            return Integer.parseInt(props.getProperty(key));
        }
    }
    public int getInt(String key) {
        if("".equals(props.getProperty(key))){
            return 0;
        }else{
            return Integer.parseInt(props.getProperty(key));
        }
    }

    public String getString(String key) {
        String value = props.getProperty(key);
        if("".equals(value)) {
            value = "";
        }
        return value;
    }
    public String getString(String key,String def) {
        String value = props.getProperty(key);
        if("".equals(value)) {
            value = def;
        }
        return value;
    }

}
