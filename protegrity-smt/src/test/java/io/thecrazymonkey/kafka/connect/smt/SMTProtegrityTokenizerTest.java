package io.thecrazymonkey.kafka.connect.smt;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisabledOnOs(value = { OS.MAC, OS.WINDOWS })
public class SMTProtegrityTokenizerTest {

        private static final Schema SCHEMA = SchemaBuilder.struct()
                .field("test1", Schema.STRING_SCHEMA)
                .field("test2", Schema.STRING_SCHEMA)
                .field("test3", Schema.STRING_SCHEMA)
                .build();
        private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA);

        static {
            VALUES_WITH_SCHEMA.put("test1", "hello world");
            VALUES_WITH_SCHEMA.put("test2", "1234567890");
            VALUES_WITH_SCHEMA.put("test3", "abcdefghijklmn");
        }

        private static SMTProtegrityTokenizer<SinkRecord> transform(List<String> values, String policy, String user, String operation) {
            final SMTProtegrityTokenizer<SinkRecord> xform = new SMTProtegrityTokenizer.Value<>();
            Map<String, Object> props = new HashMap<>();
            props.put("values", values);
            props.put("policy", policy);
            props.put("user", user);
            props.put("operation", operation);
            xform.configure(props);
            return xform;
        }

        private static SinkRecord record(Schema schema, Object value) {
            return new SinkRecord("", 0, null, null, schema, value, 0);
        }

         @Test
        public void testTokenization() {
            final List<String> fields = new ArrayList<>(SCHEMA.fields().size());
            for (Field field : SCHEMA.fields()) {
                fields.add(field.name());
            }
            List<String> testvalues = Arrays.asList("test1:test_de","test2:test_de2");
            final Struct updatedValue = (Struct) transform(testvalues, "test_policy", "ubuntu", "tokenize").apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();
            final Struct originalValue = (Struct) transform(testvalues, "test_policy", "ubuntu", "detokenize").apply(record(SCHEMA,updatedValue)).value();
            assertEquals(originalValue.get("test1"), VALUES_WITH_SCHEMA.get("test1"));
            assertEquals(originalValue.get("test2"), VALUES_WITH_SCHEMA.get("test2"));
        }

        @Test
        public void testTokenizationDefaultElement() {
            final List<String> fields = new ArrayList<>(SCHEMA.fields().size());
            for (Field field : SCHEMA.fields()) {
                fields.add(field.name());
            }
            List<String> testvalues = Arrays.asList("test1","test2:test_de2");
            final Struct updatedValue = (Struct) transform(testvalues, "test_policy", "ubuntu", "tokenize").apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();
            final Struct originalValue = (Struct) transform(testvalues, "test_policy", "ubuntu", "detokenize").apply(record(SCHEMA,updatedValue)).value();
            assertEquals(originalValue.get("test1"), VALUES_WITH_SCHEMA.get("test1"));
            assertEquals(originalValue.get("test2"), VALUES_WITH_SCHEMA.get("test2"));
        }

}
