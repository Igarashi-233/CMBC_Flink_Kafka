package com.sensorsdata.analytics.constants;

/**
 * @author igarashi233
 **/

public class CommConst {

    public enum PropertyType {
        USERPROPERTY("userProperty", "1"), MAIN_BODY("main_body", "7");
        public String type;
        public String value;

        PropertyType(String type, String value) {
            this.type = type;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }
}
