package net.khaibq.addon;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import net.khaibq.addon.model.ApiResponse;
import net.khaibq.addon.model.Output;
import net.khaibq.addon.service.DiskServiceImpl;
import net.khaibq.addon.service.RedhatServiceImpl;
import net.khaibq.addon.service.VirtualMachineServiceImpl;
import net.khaibq.addon.service.WindowServiceImpl;
import net.khaibq.addon.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.khaibq.addon.utils.CommonUtils.defaultNull;
import static net.khaibq.addon.utils.Constants.DELAY_TIME;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;
import static net.khaibq.addon.utils.Constants.UPLOAD_CSV_DB_SCHEMA_ID;
import static net.khaibq.addon.utils.Constants.UPLOAD_CSV_IMPORT_ID;

public class AiosBatchApp {
    private static final Logger logger = LogManager.getLogger(AiosBatchApp.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {
        new VirtualMachineServiceImpl().execute();
        new DiskServiceImpl().execute();
        new RedhatServiceImpl().execute();
        new WindowServiceImpl().execute();

        try {
            // Lưu file kết quả
            writeResultToFile();

            // Upload file kết quả csv
            ApiResponse uploadResponse = uploadResultToServer();

            if (uploadResponse != null && uploadResponse.getCode() == 200) {
                // Nếu upload file kết quả thành công. Sau x phút sẽ call api check kết quả
                scheduler.schedule(() -> {
                    ApiResponse checkResultResponse = callApiCheckResult(uploadResponse.getQuery().getProcessId());
                    if (checkResultResponse != null && checkResultResponse.getCode() == 200) {
                        ApiResponse.Items items = checkResultResponse.getItems();
                        String text = """
                                nowCondition: %d,
                                progress: %d,
                                succeedCount: %d,
                                failureCount: %d
                                """.formatted(items.getNowCondition(), items.getProgress(), items.getSucceedCount(), items.getFailureCount());

                        // Gửi dữ liệu sang slack
                        postSlack(text);
                    } else {
                        logger.error("Check result failed");
                    }

                }, DELAY_TIME, TimeUnit.MINUTES);

            } else {
                logger.error("Upload result file failed");
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            scheduler.shutdown();
        }

    }

    public static void writeResultToFile() {
        LocalDate localDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String localDateFmt = localDate.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));

        List<Output> outputVmList = getDataOutputFile("/vm/output-vm.csv");
        List<Output> outputDiskList = getDataOutputFile("/disk/output-disk.csv");
        List<Output> outputRedhatList = getDataOutputFile("/redhat/output-redhat.csv");
        List<Output> outputWindowList = getDataOutputFile("/window/output-window.csv");

        List<String[]> dataWriteToFile = new ArrayList<>();
        outputVmList.forEach(x -> {
            String[] s = new String[]{x.getNetworkID(), "", "aios", defaultNull(x.getPrice()), localDateFmt, "", defaultNull(x.getCount()), x.getPlan(), "", "", "", "", "", ""};
            dataWriteToFile.add(s);
        });

        outputDiskList.forEach(x -> {
            String[] s = new String[]{x.getNetworkID(), "", "aios", defaultNull(x.getPrice()), localDateFmt, "", defaultNull(x.getCount()), x.getPlan(), "", "", "", "", "", ""};
            dataWriteToFile.add(s);
        });

        outputRedhatList.forEach(x -> {
            String[] s = new String[]{x.getNetworkID(), "", "aios", defaultNull(x.getPrice()), localDateFmt, "", defaultNull(x.getCount()), "", "", "", "", "", "", ""};
            dataWriteToFile.add(s);
        });

        outputWindowList.forEach(x -> {
            String[] s = new String[]{x.getNetworkID(), "", "aios", defaultNull(x.getPrice()), localDateFmt, "", defaultNull(x.getCount()), "", "", "", "", "", "", ""};
            dataWriteToFile.add(s);
        });

        dataWriteToFile.add(0, new String[]{
                "契約サービスID", "clay請求先管理番号", "商品ID", "相対金額", "課金開始日", "課金終了日", "数量", "商品備考", "請求印字備考", "代理店 ID", "計上部署コード", "担当者コード", "商品メモ"
        });
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + "/result.csv", CharsetUtil.CHARSET_UTF_8);
        writer.write(dataWriteToFile);
    }

    private static ApiResponse callApiCheckResult(String processId) {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-HD-apitoken", Constants.X_HD_API_TOKEN);

        Map<String, Object> payload = new HashMap<>();
        payload.put("processId", processId);

        HttpRequest httpRequest = HttpUtil.createPost(Constants.API_UPLOAD_CSV_URL)
                .addHeaders(headers)
                .body(JSONUtil.toJsonStr(payload));

        var response = httpRequest.execute();
        if (response.isOk()) {
            logger.info("Call api check result success: {}", response.body());
            return JSONUtil.toBean(response.body(), ApiResponse.class);
        } else {
            throw new RuntimeException("Call api check result error: " + response.body());
        }
    }

    private static void postSlack(String text) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("text", text);

        HttpRequest httpRequest = HttpUtil.createPost(Constants.POST_SLACK_URL)
                .contentType(ContentType.JSON.getValue())
                .body(JSONUtil.toJsonStr(payload));
        var response = httpRequest.execute();
        if (response.isOk()) {
            logger.info("Call api post slack success: {}", response.body());
        } else {
            throw new RuntimeException("Call api post slack error: " + response.body());
        }
    }

    private static ApiResponse uploadResultToServer() {
        // Kiểm tra nếu có file mới thực hiện upload
        if (!FileUtil.exist(OUTPUT_DIR + "/result.csv")) {
            logger.info("Do not have file result");
            return null;
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("X-HD-apitoken", Constants.X_HD_API_TOKEN);

        Map<String, Object> json = new HashMap<>();
        json.put("dbSchemaId", UPLOAD_CSV_DB_SCHEMA_ID);
        json.put("importId", UPLOAD_CSV_IMPORT_ID);

        Map<String, Object> formMap = new HashMap<>();
        formMap.put("json", JSONUtil.toJsonStr(json));
        formMap.put("uploadFile", FileUtil.file(OUTPUT_DIR + "/result.csv"));

        HttpRequest httpRequest = HttpUtil.createPost(Constants.API_UPLOAD_CSV_URL)
                .addHeaders(headers)
                .form(formMap);
        var response = httpRequest.execute();
        if (response.isOk()) {
            logger.info("Call api upload success: {}", response.body());
            return JSONUtil.toBean(response.body(), ApiResponse.class);
        } else {
            throw new RuntimeException("Call api upload csv error: " + response.body());
        }
    }

    private static List<Output> getDataOutputFile(String filePath) {
        List<Output> list = new ArrayList<>();
        CsvReader reader = CsvUtil.getReader();
        CsvData csvData = reader.read(FileUtil.file(OUTPUT_DIR + filePath));
        List<CsvRow> rows = csvData.getRows();
        for (int i = 0; i < rows.size(); i++) {
            if (i == 0) continue;
            var row = rows.get(i);
            Output output = new Output();
            output.setNetworkID(row.get(0));
            output.setPlan(row.get(1));
            output.setCount(row.get(2) != null ? Integer.parseInt(row.get(2)) : null);
            output.setPrice(row.get(3) != null ? Integer.parseInt(row.get(3)) : null);
            list.add(output);
        }
        return list;
    }
}
