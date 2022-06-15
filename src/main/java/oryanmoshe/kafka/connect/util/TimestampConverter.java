package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.SchemaBuilder;

// Oracle fix
// >>>

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.ZoneId;

// <<<


// Native methods fix

import java.lang.reflect.Method;
import java.lang.NoSuchMethodException;
import java.lang.SecurityException;
import java.lang.IllegalAccessException;
import java.lang.reflect.InvocationTargetException;

// <<<

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Map<String, String> MONTH_MAP = Map.ofEntries(Map.entry("jan", "01"), Map.entry("feb", "02"),
            Map.entry("mar", "03"), Map.entry("apr", "04"), Map.entry("may", "05"), Map.entry("jun", "06"),
            Map.entry("jul", "07"), Map.entry("aug", "08"), Map.entry("sep", "09"), Map.entry("oct", "10"),
            Map.entry("nov", "11"), Map.entry("dec", "12"));
    public static final int MILLIS_LENGTH = 13;

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";

    public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp",
            "datetime2");

    private static final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";
    private static final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);

    public String strDatetimeFormat, strDateFormat, strTimeFormat;
    public Boolean debug;

//    private final SchemaBuilder datetimeSchema = SchemaBuilder.string().optional().name("oryanmoshe.time.DateTimeString");

    private SimpleDateFormat simpleDatetimeFormatter, simpleDateFormatter, simpleTimeFormatter;

    @Override
    public void configure(Properties props) {
        this.strDatetimeFormat = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.simpleDatetimeFormatter = new SimpleDateFormat(this.strDatetimeFormat);

        this.strDateFormat = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.simpleDateFormatter = new SimpleDateFormat(this.strDateFormat);

        this.strTimeFormat = props.getProperty("format.time", DEFAULT_TIME_FORMAT);
        this.simpleTimeFormatter = new SimpleDateFormat(this.strTimeFormat);

        this.debug = props.getProperty("debug", "false").equals("true");

        this.simpleDatetimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.simpleTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        System.out.printf(
          "[TimestampConverter.configure] Finished configuring formats. this.strDatetimeFormat: %s, this.strTimeFormat: %s%n, this.debug: %s",
          this.strDatetimeFormat, this.strTimeFormat, this.debug);

        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.configure] Finished configuring formats. this.strDatetimeFormat: %s, this.strTimeFormat: %s%n",
                    this.strDatetimeFormat, this.strTimeFormat);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.converterFor] Starting to register column. column.name: %s, column.typeName: %s, column.hasDefaultValue: %s, column.defaultValue: %s, column.isOptional: %s%n",
                    column.name(), column.typeName(), column.hasDefaultValue(), column.defaultValue(), column.isOptional());

        // Oracle fix: TIMESTAMP(4) will not be matched because of the extra width "(4)"
        // >>>

        String columnNameWithoutWidth = column.typeName().replaceAll("^(.+?)(\\(.+?\\))$", "$1");

        // <<<
        
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(columnNameWithoutWidth))) {
            System.out.printf(
              "[TimestampConverter.converterFor] Supported column. column.name: %s, column.typeName: %s, column.hasDefaultValue: %s, column.defaultValue: %s, column.isOptional: %s%n",
              column.name(), column.typeName(), column.hasDefaultValue(), column.defaultValue(), column.isOptional());

            // Use a new SchemaBuilder every time in order to avoid changing "Already set" options
            // in the schema builder between tables.
            registration.register(SchemaBuilder.string().optional(), rawValue -> {
                if (rawValue == null) {
                    // DEBUG
                    if (this.debug) {
                        System.out.printf("[TimestampConverter.converterFor] rawValue of %s is null.%n", column.name());
                    }

                    if (column.isOptional()) {
                        return null;
                    }
                    else if (column.hasDefaultValue()) {
                        return column.defaultValue();
                    }
                    return rawValue;
                }

                String nativeType = null;
                Date nativeDateObject = null;
                Method toDate = null;

                try {
                  // java.sql.Date
                  
                  toDate = rawValue.getClass().getMethod("valueOf");

                  if(toDate != null) {
                    if (toDate.getReturnType().equals(Date.class)) {
                      nativeDateObject = (Date) toDate.invoke(rawValue);
                      nativeType = "java.sql.Date";
                    }
                  }
                } catch (java.lang.NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {  
                  // nothing
                  System.out.printf("[TimestampConverter.converterFor] Not java.sql.Date %n");
                }

                try {
                  // oracle.sql.DATE, oracle.sql.TIMESTAMP

                  toDate = rawValue.getClass().getMethod("dateValue");

                  if (toDate != null) {
                    if (toDate.getReturnType().equals(Date.class)) {
                      nativeDateObject = (Date) toDate.invoke(rawValue);
                      nativeType = "oracle.sql.DATE/TIMESTAMP";
                    } 
                  }
                } catch (java.lang.NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {  
                  // nothing
                  System.out.printf("[TimestampConverter.converterFor] Not oracle.sql.DATE/TIMESTAMP %n");
                }

                try {
                  // java.time.LocalDate

                  toDate = rawValue.getClass().getMethod("atStartOfDay");

                  if (toDate != null) {
                    if (toDate.getReturnType().equals(ZonedDateTime.class)) {
                      nativeDateObject = Date.from(
                        ((ZonedDateTime) toDate.invoke(rawValue, ZoneId.of("UTC"))).toInstant()
                      );
                      nativeType = "java.time.LocalDate";
                    } 
                  }
                } catch (java.lang.NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {  
                  // nothing
                  System.out.printf("[TimestampConverter.converterFor] Not java.sql.LocalDate %n");
                }

                try {
                  // java.time.LocalDateTime

                  toDate = rawValue.getClass().getMethod("atZone");

                  if (toDate != null) {
                    if (toDate.getReturnType().equals(ZonedDateTime.class)) {
                      nativeDateObject = Date.from(
                        ((ZonedDateTime) toDate.invoke(rawValue, ZoneId.of("UTC"))).toInstant()
                      );
                      nativeType = "java.time.LocalDateTime";
                    } 
                  }
                } catch (java.lang.NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {  
                  // nothing
                  System.out.printf("[TimestampConverter.converterFor] Not java.time.LocalDateTime %n");
                }

                try {
                  // java.sql.Timestamp

                  toDate = rawValue.getClass().getMethod("toInstant");

                  if (toDate != null) {
                    if (toDate.getReturnType().equals(Instant.class)) {
                      nativeDateObject = Date.from(
                        (Instant) toDate.invoke(rawValue)
                      );
                      nativeType = "java.sql.Timestamp";
                    } 
                  }
                } catch (java.lang.NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {  
                  // nothing
                  System.out.printf("[TimestampConverter.converterFor] Not java.sql.Timestamp %n");
                }

                if (nativeType != null) {
                                  System.out.printf("[TimestampConverter.converterFor] Native mode: %s", type)
                } else {
                  System.out.printf("[TimestampConverter.converterFor] Non-native mode raw value: %s, type: %s%n", rawValue, rawValue.getClass().getSimpleName());
                }

                Instant instant = null;

                if (nativeDateObject != null) {
                  instant = nativeDateObject.toInstant();
                  if (this.debug)
                      System.out.printf(
                              "[TimestampConverter.converterFor] Native date conversion: %s",
                              column.name(), column.typeName(), instant);
                } else {
                  Long millis = getMillis(rawValue.toString(), column.typeName().toLowerCase());
                  if (millis == null)
                      return rawValue.toString();

                  instant = Instant.ofEpochMilli(millis);
                  if (this.debug)
                      System.out.printf(
                              "[TimestampConverter.converterFor] Before returning conversion. column.name: %s, column.typeName: %s, millis: %d%n",
                              column.name(), column.typeName(), millis);
                }

                Date dateObject = Date.from(instant);
                switch (column.typeName().toLowerCase()) {
                    case "time":
                        return this.simpleTimeFormatter.format(dateObject);
                    case "date":
                        System.out.printf("[TimestampConverter.converterFor] Final result: %s -> %s%n", dateObject, this.simpleDateFormatter.format(dateObject));
                        return this.simpleDateFormatter.format(dateObject);
                    default:
                        return this.simpleDatetimeFormatter.format(dateObject);
                }
            });
        }
    }

    private Long getMillis(String timestamp, String columnType) {
        if (timestamp.isBlank()) {
            System.out.printf("[TimestampConverter.getMillis] Null value: %s%n", timestamp);

            return null;
        }
        
        // Oracle fix, oracle stores original function calls to the binlog and we can no longer rely on debezium converter
        // Migrate code from: https://github.com/debezium/debezium/blob/main/debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/OracleValueConverters.java
        // >>>

        final Pattern TO_TIMESTAMP = Pattern.compile("TO_TIMESTAMP\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
        final Pattern TO_DATE = Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);

        final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("dd-MMM-yy hh.mm.ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .appendPattern(" a")
            .toFormatter(Locale.ENGLISH);

        final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .toFormatter();

        final ZoneId GMT_ZONE_ID = ZoneId.of("GMT");
        LocalDateTime dateTime;

        final Matcher toTimestampMatcher = TO_TIMESTAMP.matcher(timestamp);
        if (toTimestampMatcher.matches()) {
            String dateText = toTimestampMatcher.group(1);

            System.out.printf("[TimestampConverter.getMillis] Matched TO_TIMESTAMP: %s%n", dateText);

            if (dateText.indexOf(" AM") > 0 || dateText.indexOf(" PM") > 0) {
                dateTime = LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(dateText.trim()));
            }
            else {
                dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(dateText.trim()));
            }
            return dateTime.atZone(GMT_ZONE_ID).toInstant().toEpochMilli();
        }

        final Matcher toDateMatcher = TO_DATE.matcher(timestamp);
        if (toDateMatcher.matches()) {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(toDateMatcher.group(1)));
            
            System.out.printf("[TimestampConverter.getMillis] Matched TO_DATE: %s -> %s -> %s -> %s%n", 
              dateTime, toDateMatcher.group(1), 
              TIMESTAMP_FORMATTER.parse(toDateMatcher.group(1)),
              dateTime.atZone(GMT_ZONE_ID).toInstant().toEpochMilli());

            return dateTime.atZone(GMT_ZONE_ID).toInstant().toEpochMilli();
        }

        System.out.printf("[TimestampConverter.getMillis] Not Oracle format: %s%n", timestamp);

        // <<<

        if (timestamp.contains(":") || timestamp.contains("-")) {
            return milliFromDateString(timestamp);
        }

        // Native date fix, normally we won't hit here

        int excessLength = timestamp.length() - MILLIS_LENGTH;
        long longTimestamp = Long.parseLong(timestamp);

        if (columnType == "time") {
            System.out.printf("[TimestampConverter.getMillis] Return time directly: %s%n", longTimestamp);
            return longTimestamp;
        }

        if (excessLength < -5) {
          // Debezium-MySQL/PgSQL/SQLServer-Date = Days since epochs
          System.out.printf("[TimestampConverter.getMillis] Probably day (%s) excessLength = (%s) -> (%s)", longTimestamp, excessLength, longTimestamp * 24 * 60 * 60 * 1000);
          return longTimestamp * 24 * 60 * 60 * 1000;
        }

        if (excessLength > 0) {
          System.out.printf("[TimestampConverter.getMillis] Removing excessLength (type: %s) = %s -> %s", columnType, longTimestamp, excessLength);

          long millis = longTimestamp / (long) Math.pow(10, excessLength);
          return millis;
        }
        
        // <<<

        return longTimestamp;
    }

    private Long milliFromDateString(String timestamp) {
        System.out.printf("[TimestampConverter.milliFromDateString] Parsing: %s%n", timestamp);

        Matcher matches = regexPattern.matcher(timestamp);

        if (matches.find()) {
            String year = (matches.group("year") != null ? matches.group("year")
                    : (matches.group("year2") != null ? matches.group("year2") : matches.group("year3")));
            String month = (matches.group("month") != null ? matches.group("month")
                    : (matches.group("month2") != null ? matches.group("month2") : matches.group("month3")));
            String day = (matches.group("day") != null ? matches.group("day")
                    : (matches.group("day2") != null ? matches.group("day2") : matches.group("day3")));
            String hour = matches.group("hour") != null ? matches.group("hour") : "00";
            String minute = matches.group("minute") != null ? matches.group("minute") : "00";
            String second = matches.group("second") != null ? matches.group("second") : "00";
            String milli = matches.group("milli") != null ? matches.group("milli") : "000";

            if (milli.length() > 3)
                milli = milli.substring(0, 3);

            String dateStr = "";
            dateStr += String.format("%s:%s:%s.%s", ("00".substring(hour.length()) + hour),
                    ("00".substring(minute.length()) + minute), ("00".substring(second.length()) + second),
                    (milli + "000".substring(milli.length())));

            if (year != null) {
                if (month.length() > 2)
                    month = MONTH_MAP.get(month.toLowerCase());

                dateStr = String.format("%s-%s-%sT%sZ", year, ("00".substring(month.length()) + month),
                        ("00".substring(day.length()) + day), dateStr);
            } else {
                dateStr = String.format("%s-%s-%sT%sZ", "2020", "01", "01", dateStr);
            }

            Date dateObj = Date.from(Instant.parse(dateStr));
            return dateObj.getTime();
        }

        return null;
    }
}
