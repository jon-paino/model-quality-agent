package com.striim.opImpl;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;

import com.webaction.security.Password;
import com.webaction.security.PasswordFactory;
import com.webaction.security.PasswordFactoryGetter;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.runtime.containers.WAEvent;
import com.webaction.runtime.meta.MetaInfo.Type;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.metaRepository.MetadataRepository;
import com.webaction.security.WASecurityManager;

import oracle.sql.STRUCT;

/**
 * Partial Record Policy Open Processor Implementation
 *
 */
@PropertyTemplate(name = "PartialRecordPolicy", type = AdapterType.process, properties = {
        @PropertyTemplateProperty(name = "Username", type = String.class, required = true, defaultValue = ""),
        @PropertyTemplateProperty(name = "Password", type = Password.class, required = true, defaultValue = ""),
        @PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = ""),
        @PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
        @PropertyTemplateProperty(name = "QueryClause", type = String.class, required = false, defaultValue = ""),
        @PropertyTemplateProperty(name = "OnMissingRows", type = String.class, required = false, defaultValue = "") }, inputType = com.webaction.proc.events.WAEvent.class, outputType = com.webaction.proc.events.WAEvent.class)
public class PartialRecordPolicy extends StriimOpenProcessor {
    private static final long serialVersionUID = 1L;
    private static final String CONTEXT = "CONTEXT";
    private static final Logger logger = LogManager.getLogger(PartialRecordPolicy.class);
    private Map<String, Object> userData;
    private ContextData context = new ContextData();

    @Override
    public void start() {
        userData = new HashMap<String, Object>();
        context.properties = this.getProperties();
        if (context.properties.get("Username") == null) {
            throw new RuntimeException("Username not specified.");
        }
        if (context.properties.get("ConnectionURL") == null) {
            throw new RuntimeException("Connection URL not specified.");
        }
        if (context.properties.get("Password") == null) {
            throw new RuntimeException("Password not specified.");
        }
        if (context.properties.get("Tables") == null) {
            throw new RuntimeException("Tables not specified.");
        }
        context.username = context.properties.get("Username").toString();
        try {
            PasswordFactory passwordFactory = PasswordFactoryGetter.getPasswordFactory();
            context.password = passwordFactory.getPasswordObject().extractPassword(context.properties.get("Password"));
        } catch(Exception ex) {
            throw new RuntimeException("Failed to extract Password.");
        }
        context.url = context.properties.get("ConnectionURL").toString();
        context.tableString = context.properties.get("Tables").toString().trim();
        createConnection();
        setFetchColumns();
        setOffset();
        userData.put("CONTEXT", context);
    }

    public void run() {
        context = (ContextData) userData.get(CONTEXT);
        if(context == null)
            throw new RuntimeException("Unable to resolve the initialised params. (context)");
        Iterator<WAEvent> incomingEvents = getAdded().iterator();
        while (incomingEvents.hasNext()) {
            WAEvent thisEvent = incomingEvents.next();
            com.webaction.proc.events.WAEvent out = com.webaction.proc.events.WAEvent
                    .makeCopy((com.webaction.proc.events.WAEvent) thisEvent.data);
            try {
                if ("DELETE".equalsIgnoreCase((String) out.metadata.get("OperationName"))) {
                    send(out);
                    continue;
                }
                if (context.tableName.equalsIgnoreCase((String) out.metadata.get("TableName"))) {
                    if (context.cachedTypeObj == null)
                        context.cachedTypeObj = (Type) MetadataRepository.getINSTANCE()
                                .getMetaObjectByUUID(out.typeUUID, WASecurityManager.get().getTOKEN());

                    context.keyColumns = context.cachedTypeObj.keyFields;
                    if (context.keyColumns.size() == 0) {
                        context.keyColumns = context.columns;
                    }

                    String fetchColString;
                    String[] fetchCols = new String[context.fetchColumns.length];

                    /*
                     * The following loop would construct different parameters according to the type
                     * of column to be fetched. As in Oracle XML type, we need to use a method
                     * getClobVal() with an alias for fetching XML columns.
                     */
                    for (int i = 0; i < context.fetchColumns.length; i++) {
                        if (context.colType.containsKey(context.fetchColumns[i].toUpperCase()) && context.colType.get(context.fetchColumns[i].toUpperCase()).equalsIgnoreCase("SYS.XMLTYPE"))
                            fetchCols[i] = "ab." + context.fetchColumns[i] + ".getClobVal() " + context.fetchColumns[i];
                        else
                            fetchCols[i] = "ab." + context.fetchColumns[i] + " " + context.fetchColumns[i];
                    }
                    fetchColString = String.join(",", fetchCols);
                    String query = "SELECT " + fetchColString + " from " + context.tableName + " ab ";
                    query += " where ";

                    List<String> cols = new ArrayList<String>();
                    for (int i = 0; i < context.keyColumns.size(); i++) {
                        if (!isFetchCol(context.keyColumns.get(i))) {
                            int offset = getOffset(context.keyColumns.get(i));
                            cols.add(" " + context.keyColumns.get(i) + " = '" + out.data[offset] + "' ");
                        }
                    }
                    query += String.join("and", cols);
                    try (PreparedStatement stmt = context.connection.prepareStatement(query);
                         ResultSet result = stmt.executeQuery()) {
                        result.next();
                        ResultSetMetaData rsmd = result.getMetaData();
                        for (int i = 0; i < context.fetchColumns.length; i++) {
                            if (rsmd.getColumnTypeName(i + 1).equals("MDSYS.SDO_GEOMETRY")) {
                                /*
                                 * If the column is a ORACLE SDO datatype, we need special classes to retrieve
                                 * the data and convert it into JSON String.
                                 */
                                try {
                                    GsonBuilder gsonBuilder = new GsonBuilder().serializeSpecialFloatingPointValues();
                                    Gson gson = gsonBuilder.create();
                                    STRUCT st = (oracle.sql.STRUCT) result.getObject(context.fetchColumns[i]);
                                    Class<?> jGeo = Class.forName("oracle.spatial.geometry.JGeometry");
                                    Object jfi = jGeo.getDeclaredMethod("load", oracle.sql.STRUCT.class).invoke(null, st);
                                    String columnValue = gson.toJson(jfi);
                                    String jsonColumn = columnValue.replaceAll("NaN", "null");
                                    int offset = getOffset(context.fetchColumns[i]);
                                    out.setData(offset, jsonColumn);
                                } catch (ExceptionInInitializerError | ClassNotFoundException | NoSuchMethodException  e) {
                                    logger.warn("Unable to load jgeometry class. Spatial column will not be fetched.");
                                }
                            } else if(rsmd.getColumnTypeName(i + 1).equals("BLOB")) {
                                /*
                                 * In case of BLOB data, fetch the hex representation of the data.
                                 */
                                String columnValue="";
                                byte[] bytes=result.getBytes((context.fetchColumns[i]));
                                if(bytes!=null){
                                    String data =DatatypeConverter.printHexBinary(bytes);
                                    columnValue = data;
                                }
                                int offset = getOffset(context.fetchColumns[i]);
                                out.setData(offset, columnValue);
                            }
                            else {
                                String columnValue = result.getString(context.fetchColumns[i]);
                                int offset = getOffset(context.fetchColumns[i]);
                                out.setData(offset, columnValue);
                            }
                        }
                    }
                    send(out);
                } else {
                    continue;
                }
            } catch (Exception e) {
                logger.warn("Could not fetch columns from source table. The WAEvent has not been modified."
                        + e.getMessage());
                send(out);
            }
        }
        userData.put(CONTEXT, context);
    }

    private boolean isFetchCol(String col1) {
        for (int i = 0; i < context.fetchColumns.length; i++) {
            if (context.fetchColumns[i].toString().trim().equalsIgnoreCase(col1)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        context = (ContextData) userData.get(CONTEXT);
        if(context != null) {
            if (context.connection != null && !context.connection.isClosed()) {
                context.connection.close();
                context.connection = null;
            }
            userData.put(CONTEXT, context);
        }
    }

    private void setFetchColumns() throws RuntimeException {
        try {
            context.tableName = context.tableString.substring(0, context.tableString.indexOf("("));
            String cols = context.tableString.substring(context.tableString.indexOf("(") + 1,
                    context.tableString.indexOf(")"));
            context.fetchColumns = cols.split(",");
        } catch (Exception e) {
            throw new RuntimeException("Tables not correctly specified");
        }
    }

    public Map getAggVec() {
        return userData;
    }

    public void setAggVec(Map aggVec) {
        userData = aggVec;
    }

    public void createConnection() {
        try {
            context.connection = DriverManager.getConnection(context.url, context.username, context.password);
        } catch (SQLException e) {
            String errorString = " Failure in connecting to Database with url : " + context.url + " username : "
                    + context.username + " \n ErrorCode : " + e.getErrorCode() + ";" + "SQLCode : " + e.getSQLState()
                    + ";" + "SQL Message : " + e.getMessage();
            RuntimeException exception = new RuntimeException(errorString);
            throw exception;
        } catch (Exception e) {
            String errorString = " Failure in connecting to Database with url : " + context.url + " username : "
                    + context.username + " \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
            RuntimeException exception = new RuntimeException(errorString);
            throw exception;
        }
    }

    public void setOffset() {
        String query = "select * from " + context.tableName;
        try (Statement ps = context.connection.createStatement()) {
            ps.setMaxRows(1);
            try (ResultSet meta = ps.executeQuery(query)) {
                ResultSetMetaData rsmd = meta.getMetaData();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    context.colOffset.put(rsmd.getColumnName(i + 1).toString().toUpperCase(), i);
                    context.colType.put(rsmd.getColumnName(i + 1).toString().toUpperCase(),
                            rsmd.getColumnTypeName(i + 1));
                    context.columns.add(rsmd.getColumnName(i + 1).toString().toUpperCase());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to fetch table '%s' details : ", context.tableName) + e.getMessage());
        }
    }

    public int getOffset(String keyCol) {
        return context.colOffset.get(keyCol.toUpperCase());
    }
}