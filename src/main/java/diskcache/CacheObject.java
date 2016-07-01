package diskcache;

import java.io.Serializable;


public class CacheObject implements Serializable {
    private String stringField;
    private int intField;

    private static final long serialVersionUID = 936540265026540L;

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public int getIntField() {
        return intField;
    }

    public void setIntField(int intFields) {
        this.intField = intFields;
    }

    public String toString() {
        return stringField + "," + intField;
    }
}
