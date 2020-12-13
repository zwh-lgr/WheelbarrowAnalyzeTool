import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeParser {
    public static LocalDateTime parseToLocalDateTime(String textTime) {
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime ldt = LocalDateTime.parse(textTime, f);

        return ldt;
    }
}
