package oryanmoshe.kafka.connect.util;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.spi.converter.RelationalColumn;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

import java.util.OptionalInt;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.ZoneId;
import java.sql.Date;
import oracle.sql.DATE;
import oracle.sql.TIMESTAMP;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

public class TimestampConverterTests {
    private class MockRegistration<S> implements ConverterRegistration<S> {
        public Converter _converter;
        public S _schema;

        @Override
        public void register(S fieldSchema, Converter converter) {
            _converter = converter;
            _schema = fieldSchema;
        }
    }

    @ParameterizedTest
    @CsvSource({ "YY-MM-dd, YY-MM-dd", "," + TimestampConverter.DEFAULT_DATE_FORMAT })
    void configureDateTest(final String inputFormat, final String expectedFormat) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (inputFormat != null)
            props.put("format.date", inputFormat);

        final String beforeConfig = tsConverter.strDateFormat;
        assertEquals(null, beforeConfig, beforeConfig + " before configuration, should equal " + null);
        System.out.println(beforeConfig);

        tsConverter.configure(props);

        final String actualResult = tsConverter.strDateFormat;
        assertEquals(expectedFormat, actualResult,
                actualResult + " after configuration, should equal " + expectedFormat);
        System.out.println(actualResult);
    }

    @ParameterizedTest
    @CsvSource({ "mm:ss.SSS, mm:ss.SSS", "," + TimestampConverter.DEFAULT_TIME_FORMAT })
    void configureTimeTest(final String inputFormat, final String expectedFormat) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (inputFormat != null)
            props.put("format.time", inputFormat);

        final String beforeConfig = tsConverter.strTimeFormat;
        assertEquals(null, beforeConfig, beforeConfig + " before configuration, should equal " + null);
        System.out.println(beforeConfig);

        tsConverter.configure(props);

        final String actualResult = tsConverter.strTimeFormat;
        assertEquals(expectedFormat, actualResult,
                actualResult + " after configuration, should equal " + expectedFormat);
        System.out.println(actualResult);
    }

    @ParameterizedTest
    @CsvSource(value = { "date| | TO_DATE('2021-12-31 13:27:18', 'YYYY-MM-DD HH24:MI:SS') | 2021-12-31",
            "timestamp| yyyy-MM-dd | TO_TIMESTAMP('2000-12-31 13:27:18') | 2000-12-31",
            "datetime | yyyy-MM-dd | 1640936125000 | 2021-12-31",
            "datetime | YYYY-MM-dd | 915089725000 | 1999-12-31", // made wrong
            "date| yyyy-MM-dd| 18368| 2020-04-16", 
            "date|| 18368| 2020-04-16", 
            "time| mm:ss.SSS| 2230| 00:02.230",
            "time|| 2230| 00:00:02.230", 
            "datetime| yyyy-MM-dd| 1587042000279| 2020-04-16",
            "datetime|| 1587042000279| 2020-04-16T13:00:00.279Z",
            "timestamp| yyyy-MM-dd| 1587042000279| 2020-04-16",
            "datetime2|| 1587042000279| 2020-04-16T13:00:00.279Z",
            "datetime2| yyyy-MM-dd| 1587042000279| 2020-04-16",
            "timestamp|| 1587042000279| 2020-04-16T13:00:00.279Z",
            "date| yyyy-MM-dd| 2019-04-19| 2019-04-19",
            "datetime|| 2019-04-19 15:13:20.345123| 2019-04-19T15:13:20.345Z",
            "time|| 15:13:20| 15:13:20.000",
            "time|HH:mm:ss| 15:13:20| 15:13:20",
            "timestamp|| 2019-04-19 15:13:20| 2019-04-19T15:13:20.000Z",
            "datetime|| 19-Apr-2019 15:13:20.345123| 2019-04-19T15:13:20.345Z",
            "datetime|| 19/04/2019 15:13:20.345123| 2019-04-19T15:13:20.345Z",
            "datetime|| 2019-4-19 15:13:20.345123| 2019-04-19T15:13:20.345Z",
            "datetime2|| 2019-4-19 15:13:20.345123| 2019-04-19T15:13:20.345Z",
            "datetime|| 2019-4-19 3:1:0.345123| 2019-04-19T03:01:00.345Z", 
            "datetime|yyyy-MM-dd||", 
            "timestamp|||", "date|||"
            }, delimiter = '|')
    void converterTest(final String columnType, final String format, final String input, final String expectedResult) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (format != null)
            props.put(
                    String.format("format.%s",
                            columnType.equals("timestamp") || columnType.equals("datetime2") ? "datetime" : columnType),
                    format);
        props.put("debug", "true");
        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        System.out.println(mockRegistration._schema.name());

        Object actualResult = mockRegistration._converter.convert(input);
        System.out.printf("Actual result: %s%n", actualResult);
        if (actualResult == null) {
            assertEquals(actualResult, null, String.format(
                    "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                    columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
        } else {
        assertEquals(expectedResult, actualResult,
                String.format(
                        "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                        columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
        }
    }
    
    @ParameterizedTest
    @MethodSource("generateNativeDateObjects")
    void converterTestNative(final String columnType, final String format, final Object input, final String expectedResult) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (format != null)
            props.put(
                    String.format("format.%s",
                            columnType.equals("timestamp") || columnType.equals("datetime2") ? "datetime" : columnType),
                    format);
        props.put("debug", "true");
        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        System.out.println(mockRegistration._schema.name());

        Object actualResult = mockRegistration._converter.convert(input);
        System.out.printf("Actual result: %s%n", actualResult);
        if (actualResult == null) {
            assertEquals(actualResult, null, String.format(
                    "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                    columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
        } else {
        assertEquals(expectedResult, actualResult,
                String.format(
                        "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                        columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
        }
    }

    static Stream<Arguments> generateNativeDateObjects() throws SQLException {
      // final String columnType, final String format, final String input, final String expectedResult

      DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
      DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

      return Stream.of(
          // will given wrong dates for 1000 or 1700 because of calendar adjustments, will have a few days diff
          Arguments.of("date", "yyyy-MM-dd", LocalDate.parse("1900-12-31", dateFormatter), "1900-12-31"),
          Arguments.of("date", "yyyy-MM-dd", LocalDateTime.parse("1882-06-15 10:12:00", dateTimeFormatter), "1882-06-15"),
          Arguments.of("date", "yyyy-MM-dd", new java.sql.Date(1640936125000L), "2021-12-31"),
          Arguments.of("datetime", "yyyy-MM-dd HH.mm.ss", new oracle.sql.DATE(new java.sql.Date(-126359959000L), Calendar.getInstance(TimeZone.getTimeZone("GMT"))), "1965-12-30 12.00.41"),
          Arguments.of("date", "yyyy-MM-dd HH:mm:ss", new oracle.sql.TIMESTAMP(new java.sql.Timestamp(-469108759000L), Calendar.getInstance(TimeZone.getTimeZone("GMT"))),"1955-02-19 12:00:41")
      );
    }

    RelationalColumn getMockColumn(String type) {
        switch (type) {
            case "date":
                return getDateColumn();
            case "time":
                return getTimeColumn();
            case "datetime":
                return getDateTimeColumn();
            case "datetime2":
                return getDateTime2Column();
            case "timestamp":
                return getTimestampColumn();
            default:
                return null;
        }
    }

    RelationalColumn getDateColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "date";
            }

            @Override
            public String name() {
                return "datecolumn";
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getTimeColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "time";
            }

            @Override
            public String name() {
                return "timecolumn";
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getDateTimeColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "datetime";
            }

            @Override
            public String name() {
                return "datetimecolumn";
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return true;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getDateTime2Column() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "datetime2";
            }

            @Override
            public String name() {
                return "datetime2column";
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getTimestampColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "timestamp";
            }

            @Override
            public String name() {
                return "timestampcolumn";
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }
}