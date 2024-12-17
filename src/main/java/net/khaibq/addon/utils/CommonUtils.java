package net.khaibq.addon.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.khaibq.addon.model.Output;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static net.khaibq.addon.utils.Constants.BACKUP_DIR;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommonUtils {


    public static String getString(ResultSet rs, String attr) throws SQLException {
        return rs.getString(attr);
    }

    public static Integer getInteger(ResultSet rs, String attr) throws SQLException {
        if (rs.getObject(attr) != null) {
            return rs.getInt(attr);
        }
        return null;
    }

    public static Integer getInteger(String str) {
        if (CharSequenceUtil.isBlank(str)) return null;
        return Integer.valueOf(str);
    }

    public static Long getLong(ResultSet rs, String attr) throws SQLException {
        if (rs.getObject(attr) != null) {
            return rs.getLong(attr);
        }
        return null;
    }

    public static Double getDouble(ResultSet rs, String attr) throws SQLException {
        if (rs.getObject(attr) != null) {
            return rs.getDouble(attr);
        }
        return null;
    }

    public static LocalDate getLocalDate(ResultSet rs, String attr) throws SQLException {
        if (rs.getObject(attr) != null) {
            return rs.getTimestamp(attr).toLocalDateTime().toLocalDate();
        }
        return null;
    }

    public static LocalDate getLocalDate(String str) {
        if (str == null) return null;
        if (Pattern.matches("\\d{4}/\\d{2}", str)) {
            str += "/01";
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        return LocalDate.parse(str, formatter);
    }

    public static LocalDateTime getLocalDateTime(ResultSet rs, String attr) throws SQLException {
        if (rs.getObject(attr) != null) {
            return rs.getTimestamp(attr).toLocalDateTime();
        }
        return null;
    }

    public static String defaultNull(Integer input) {
        if (input == null) return null;
        return String.valueOf(input);
    }

    public static String defaultNull(Long input) {
        if (input == null) return null;
        return String.valueOf(input);
    }

    public static String defaultNull(Double input) {
        if (input == null) return null;
        return String.valueOf(input);
    }

    public static void writeToOutputFile(List<Output> outputList, String fileName) {
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + fileName, CharsetUtil.CHARSET_UTF_8);
        List<String[]> dataWriteToFile = outputList.stream()
                .sorted(Comparator.comparing(Output::getNetworkID))
                .map(x -> new String[]{x.getNetworkID(), x.getPlan(), String.valueOf(x.getCount()), String.valueOf(x.getPrice())})
                .collect(Collectors.toList());
        dataWriteToFile.add(0, new String[]{"networkID", "plan", "count", "price"});
        writer.write(dataWriteToFile);
    }

    public static void backupFile(String path, String type, List<String> filenames) {
        filenames.forEach(filename -> backupFile(path, type, filename));
    }

    public static void backupFile(String path, String type, String filename) {
        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now());
        String srcPath = path + File.separator + filename;
        String destPath = BACKUP_DIR + File.separator + date + File.separator + type + File.separator + filename;
        FileUtil.copy(srcPath, destPath, true);
    }
}
