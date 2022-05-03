package laoflch.debezium.connector.informix;

public interface Nullable {

    /**
     * @return true if this object has real value, false if it is NULL object
     */
    boolean isAvailable();

}
