package io.thecrazymonkey.kafka.connect.smt;

import com.protegrity.ap.java.Protector;
import com.protegrity.ap.java.ProtectorException;
import com.protegrity.ap.java.SessionObject;
import com.protegrity.ap.java.SessionTimeoutException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class SMTProtegrityTokenizer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Tokenize the record field(s) using the Protegrity Tokenizer."
            + "<p/>Under the hood, the tokenizer calls Protegrity API. "
            + "If the record field matches the configuration it is tokenized using Data Elemement from the configuration.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.OPERATION, ConfigDef.Type.STRING, "tokenize", ConfigDef.Importance.HIGH,
                    "Operation on data tokenize or detokenize.")
            .define(ConfigName.VALUE_FIELDS, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "Fields in a format of NAME:DATAELEMENT_EX,NAME:DATAELEMENT_Y.")
            .define(ConfigName.POLICY_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "Policy to use for default element retrieval.")
            .define(ConfigName.USER_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "User associated with the policy.");

    private interface ConfigName {
        String VALUE_FIELDS = "values";
        String POLICY_NAME = "policy";
        String USER_NAME = "user";
        String OPERATION = "operation";
    }

    private static final String PURPOSE = "tokenize fields";
    private Protector protector;
    private SessionObject session;
    private Map<String, String> fieldElement;
    private String username;
    private Boolean operation;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        String defaultElement;
        username = config.getString(ConfigName.USER_NAME);
        operation = config.getString(ConfigName.OPERATION).equals("tokenize");
        try {
            protector = Protector.getProtector();
            session = protector.createSession(username);
            System.err.println("Protegrity API version: " + protector.getVersion());
            // read the default element if set
            defaultElement = protector.getDefaultDataelementName(session, config.getString(ConfigName.POLICY_NAME));
            // if (StringUtils.isEmpty(defaultElement))
            //    System.err.println("Default element : " + protector.getLastError(session));
        } catch (ProtectorException e) {
            System.err.println(e.toString());
            // die if unable to create the Protector
            throw new RuntimeException(e);
        }
        fieldElement = new HashMap<>();
        Set<String> tokenizedFields = new HashSet<>(config.getList(ConfigName.VALUE_FIELDS));
        for (String field : tokenizedFields) {
            String[] keyValue = field.split(":");
            if (keyValue.length == 2) {
                fieldElement.put(keyValue[0], keyValue[1]);
            } else {
                fieldElement.put(keyValue[0], defaultElement);
            }
        }
        System.err.println("Token map:" + fieldElement);
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (schema != null) {
            final Struct value = requireStruct(operatingValue(record), PURPOSE);
            final Struct updatedValue = new Struct(value.schema());

            for (Field field : value.schema().fields()) {
                final Object origFieldValue = value.get(field);
                // tokenize and add to new record
                updatedValue.put(field, fieldElement.containsKey(field.name()) ? tokenizer(origFieldValue, fieldElement.get(field.name())) : origFieldValue);
            }
            return newRecord(record, updatedValue);
        } else {
            final Map<String,Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
            final Map<String,Object> updatedValue = new HashMap<>(value.size());
            for (Map.Entry<String,Object> field : value.entrySet()){
                final Object origFieldValue = field.getValue();
                final String fieldName = field.getKey();
                // tokenize and add to new record
                updatedValue.put(fieldName, fieldElement.containsKey(fieldName) ? tokenizer(origFieldValue, fieldElement.get(fieldName)) : origFieldValue);
            }
            return newRecord(record, updatedValue);
        }
    }

    private Object tokenizer(Object origFieldValue, String element) {
        String[] inputValue = new String[1];
        String[] outputValue = new String[1];
        inputValue[0] = (String) origFieldValue;
        try {
            if (operation) {
                if (!protector.protect(session, element, inputValue, outputValue)) {
                    final String error = protector.getLastError(session);
                    System.err.println("Protegrity protect : " + error);
                    // die if unable to create the Protector
                    throw new RuntimeException(error);
                }
            } else {
                if (!protector.unprotect(session, element, inputValue, outputValue)) {
                    final String error = protector.getLastError(session);
                    System.err.println("Protegrity unprotect : " + error);
                    // die if unable to create the Protector
                    throw new RuntimeException(error);
                }
            }
        } catch (SessionTimeoutException e) {
            System.err.println(e.toString());
            try {
                session = protector.createSession(username);
                // re-try
                if (operation) {
                    if (!protector.protect(session, element, inputValue, outputValue)) {
                        final String error = protector.getLastError(session);
                        System.err.println("Protegrity protect : " + error);
                        // die if unable to create the Protector
                        throw new RuntimeException(error);
                    }
                } else {
                    if (!protector.unprotect(session, element, inputValue, outputValue)) {
                        final String error = protector.getLastError(session);
                        System.err.println("Protegrity unprotect : " + error);
                        // die if unable to create the Protector
                        throw new RuntimeException(error);
                    }
                }
            } catch (ProtectorException ex) {
                // something bad - can't reconnect
                System.err.println(ex.toString());
                throw new RuntimeException(ex);
            }
        } catch (ProtectorException e) {
            System.err.println(e.toString());
            throw new RuntimeException(e);
        }
        return outputValue[0];
    }

    @Override
    public void close() {
        protector = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends SMTProtegrityTokenizer<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                    updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends SMTProtegrityTokenizer<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    record.valueSchema(), updatedValue, record.timestamp());
        }

    }

}