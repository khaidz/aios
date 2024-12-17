package net.khaibq.addon.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Tuple;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import net.khaibq.addon.model.BasicPrice;
import net.khaibq.addon.model.NonCharge;
import net.khaibq.addon.model.Output;
import net.khaibq.addon.model.WindowModel;
import net.khaibq.addon.utils.CommonUtils;
import net.khaibq.addon.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.khaibq.addon.utils.CommonUtils.defaultNull;
import static net.khaibq.addon.utils.CommonUtils.writeToOutputFile;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;
import static net.khaibq.addon.utils.Constants.WINDOW_DIR;

public class WindowServiceImpl implements BaseService {
    private static final Logger logger = LogManager.getLogger(WindowServiceImpl.class);

    @Override
    public void execute() {

        logger.info("===== Start process Window =====");
        MasterService masterService = new MasterServiceImpl();

        // Kiểm tra dữ liệu master có hợp lệ hay không. Nếu không hợp lệ, dừng chương trình ngay
        if (!masterService.isValidMasterData()) {
            logger.info("Dữ liệu master data không hợp lệ. Chương trình kết thúc!");
            return;
        }

        try {
            // Lấy ra dữ liệu các bảng master
            List<BasicPrice> basicPriceList = masterService.getAllBasicPrice();
            List<NonCharge> nonChargeList = masterService.getAllNonCharge();

            // Đọc dữ liệu các file
            List<WindowModel> windowList = getWindowModel(WINDOW_DIR);

            List<WindowModel> transformListAll = windowList.stream().map(redhat -> {
                redhat.setIsValid(true);

                handleCalcType(redhat, nonChargeList, basicPriceList);

                return redhat;
            }).collect(Collectors.toList());

            List<WindowModel> transformListNonCharge = transformListAll.stream()
                    .filter(x -> Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Và loại bỏ những redhat free
            List<WindowModel> transformList = transformListAll.stream()
                    .filter(x -> !Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Danh sách các dòng redhat không hợp lệ
            List<WindowModel> invalidList = transformList.stream().filter(x -> !x.getIsValid()).collect(Collectors.toList());

            // Danh sách các dòng redhat hợp lệ
            List<WindowModel> validList = transformList.stream().filter(WindowModel::getIsValid).toList();

            // Lọc lại 1 lần nữa các networkId có trong invalidList và validList -> không tính toán các networkId này
            List<String> invalidNetworkIds = invalidList.stream().map(WindowModel::getNetworkID).distinct().toList();
            invalidList.addAll(validList.stream().filter(x -> invalidNetworkIds.contains(x.getNetworkID())).toList());

            List<WindowModel> finalValidList = validList.stream()
                    .filter(x -> !invalidNetworkIds.contains(x.getNetworkID())).collect(Collectors.toList());

            // Nhóm các vm theo network
            Map<Tuple, List<WindowModel>> groupNetwork = finalValidList.stream()
                    .collect(Collectors.groupingBy(vm -> new Tuple(vm.getNetworkID())));

            List<Output> outputList = new ArrayList<>();

            groupNetwork.keySet()
                    .forEach(key -> {
                        List<WindowModel> list = groupNetwork.get(key);
                        Output output = new Output();
                        output.setNetworkID(key.get(0));
                        output.setPlan(null);
                        output.setCount(list.size());
                        output.setPrice(list.get(0).getPrice());
                        outputList.add(output);
                    });

            // Loại bỏ những giá trị count = 0 hoặc price = 0
            List<Output> finalOutput = outputList.stream()
                    .filter(x -> x.getCount() != null && x.getCount() > 0 && x.getPrice() != null && x.getPrice() > 0)
                    .toList();

            writeToOutputFile(finalOutput, "/window/output-window.csv");

            writeToWindowFile(invalidList, "/window/invalid-window.csv");

            //Thêm danh sách non charge vào cuối
            List<WindowModel> combinedValidAndNonCharge = new ArrayList<>();
            combinedValidAndNonCharge.addAll(finalValidList);
            combinedValidAndNonCharge.addAll(transformListNonCharge);
            writeToWindowFile(combinedValidAndNonCharge, "/window/valid-window.csv");
            logger.info("===== End process Window =====");

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("===== Exception in process Window =====");
            logger.info("========== Reason: {}", e.getMessage());
        }
    }

    private List<WindowModel> getWindowModel(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileVolume = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("windowsos_"))
                .toList();

        CommonUtils.backupFile(path, "window", listFileVolume);

        List<WindowModel> list = new ArrayList<>();

        for (String fileNameVolume : listFileVolume) {
            CsvReader reader = CsvUtil.getReader();
            CsvData csvData = reader.read(FileUtil.file(path + File.separator + fileNameVolume));
            List<CsvRow> rows = csvData.getRows();

            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                WindowModel windowModel = new WindowModel();
                windowModel.setVmId(row.get(0));
                windowModel.setGuestOsId(row.get(1));
                windowModel.setName(row.get(2));
                windowModel.setPath(row.get(3));
                windowModel.setNetworkID(windowModel.getPath().split("/")[1]);
                list.add(windowModel);
            }
        }
        return list;
    }

    private void handleCalcType(WindowModel window, List<NonCharge> nonChargeList, List<BasicPrice> basicPriceList) {
        LocalDate previous = LocalDate.now().minusMonths(1).withDayOfMonth(1);

        var nonCharge = nonChargeList.stream()
                .filter(x -> Objects.equals(x.getNetworkID(), window.getNetworkID())
                             && Objects.equals(x.getUuid(), window.getVmId())
                             && Objects.equals(x.getOsFlag(), 0)
                             && x.getStartTime().compareTo(previous) <= 0 && x.getStopTime().compareTo(previous) >= 0)
                .findFirst().orElse(null);
        if (nonCharge != null) {
            window.setCalcType(Constants.CALC_FREE_TYPE);
        } else {
            window.setCalcType(Constants.CALC_BASIC_TYPE);
            var basicPrice = basicPriceList.stream()
                    .filter(x -> Objects.equals(x.getNetworkID(), window.getNetworkID()))
                    .findFirst().orElse(null);
            if (basicPrice != null) {
                window.setPrice(basicPrice.getUnitPriceWindowServer());
            } else {
                // Trường hợp không tìm thấy basic price thì đánh dấu là không hợp lệ
                window.setIsValid(false);
                window.setInvalidReason("Not found basic price");
            }
        }
    }

    private void writeToWindowFile(List<WindowModel> list, String filename) {
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + File.separator + filename, CharsetUtil.CHARSET_UTF_8);
        List<String[]> dataWriteToFile = list.stream()
                .map(x -> new String[]{
                        x.getNetworkID(),
                        x.getVmId(),
                        x.getGuestOsId(),
                        x.getName(),
                        x.getPath(),
                        defaultNull(x.getCalcType()),
                        defaultNull(x.getPrice()),
                        defaultNull(x.getCount()),
                        String.valueOf(x.getIsValid()),
                        x.getInvalidReason()
                })
                .collect(Collectors.toList());
        dataWriteToFile.add(0, new String[]{
                "networkID",
                "vmId",
                "guestOsId",
                "name",
                "path",
                "calcType",
                "price",
                "count",
                "isValid",
                "invalidReason"
        });
        writer.write(dataWriteToFile);
    }
}
