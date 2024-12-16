package net.khaibq.addon.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import net.khaibq.addon.model.BasicPrice;
import net.khaibq.addon.model.NonCharge;
import net.khaibq.addon.model.Output;
import net.khaibq.addon.model.RedhatModel;
import net.khaibq.addon.utils.CommonUtils;
import net.khaibq.addon.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.khaibq.addon.utils.CommonUtils.defaultNullIfEmpty;
import static net.khaibq.addon.utils.CommonUtils.writeToOutputFile;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;
import static net.khaibq.addon.utils.Constants.REDHAT_DIR;

public class RedhatServiceImpl implements BaseService {
    private static final Logger logger = LogManager.getLogger(RedhatServiceImpl.class);

    @Override
    public void execute() {
        logger.info("===== Start process Redhat =====");
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
            List<RedhatModel> redhatList = getRedhatModel(REDHAT_DIR);

            List<RedhatModel> transformListAll = redhatList.stream().map(redhat -> {
                redhat.setIsValid(true);

                handleCalcType(redhat, nonChargeList, basicPriceList);

                return redhat;
            }).collect(Collectors.toList());

            List<RedhatModel> transformListNonCharge = transformListAll.stream()
                    .filter(x -> Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Và loại bỏ những redhat free
            List<RedhatModel> transformList = transformListAll.stream()
                    .filter(x -> !Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Danh sách các dòng redhat không hợp lệ
            List<RedhatModel> invalidList = transformList.stream().filter(x -> !x.getIsValid()).collect(Collectors.toList());

            // Danh sách các dòng redhat hợp lệ
            List<RedhatModel> validList = transformList.stream().filter(RedhatModel::getIsValid).toList();

            // Lọc lại 1 lần nữa các networkId có trong invalidList và validList -> không tính toán các networkId này
            List<String> invalidNetworkIds = invalidList.stream().map(RedhatModel::getNetworkID).distinct().toList();
            invalidList.addAll(validList.stream().filter(x -> invalidNetworkIds.contains(x.getNetworkID())).toList());

            List<RedhatModel> finalValidList = validList.stream()
                    .filter(x -> !invalidNetworkIds.contains(x.getNetworkID())).toList();

            List<Output> outputList = new ArrayList<>();
            finalValidList.forEach(redhat -> {
                Output output = new Output();
                output.setNetworkID(redhat.getNetworkID());
                output.setPlan(null);
                output.setCount(redhat.getCount());
                output.setPrice(redhat.getPrice());
                outputList.add(output);
            });

            // Loại bỏ những giá trị count = 0 hoặc price = 0
            List<Output> finalOutput = outputList.stream()
                    .filter(x -> x.getCount() != null && x.getCount() > 0 && x.getPrice() != null && x.getPrice() > 0)
                    .toList();

            writeToOutputFile(finalOutput, "/redhat/output-redhat.csv");

            writeToRedhatFile(invalidList, "/redhat/invalid-redhat.csv");

            //Thêm danh sách non charge vào cuối
            List<RedhatModel> combinedValidAndNonCharge = new ArrayList<>();
            combinedValidAndNonCharge.addAll(finalValidList);
            combinedValidAndNonCharge.addAll(transformListNonCharge);
            writeToRedhatFile(combinedValidAndNonCharge, "/redhat/valid-redhat.csv");
            logger.info("===== End process Redhat =====");

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("===== Exception in process Redhat =====");
            logger.info("========== Reason: {}", e.getMessage());
        }
    }

    private List<RedhatModel> getRedhatModel(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileVolume = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("rhelos_"))
                .toList();

        CommonUtils.backupFile(path, "redhat", listFileVolume);

        List<RedhatModel> list = new ArrayList<>();

        for (String fileNameVolume : listFileVolume) {
            CsvReader reader = CsvUtil.getReader();
            CsvData csvData = reader.read(FileUtil.file(path + File.separator + fileNameVolume));
            List<CsvRow> rows = csvData.getRows();

            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                RedhatModel redhatModel = new RedhatModel();
                redhatModel.setVmId(row.get(0));
                redhatModel.setVmName(row.get(1));
                redhatModel.setInstanceName(row.get(2));
                redhatModel.setDomain(row.get(3));
                redhatModel.setGuestOsId(row.get(4));
                redhatModel.setTemplateUuid(row.get(5));
                redhatModel.setVCpu(row.get(6) != null ? Integer.parseInt(row.get(6)) : null);
                redhatModel.setNetworkID(redhatModel.getDomain().split("/")[1]);
                list.add(redhatModel);
            }
        }
        return list;
    }

    private void handleCalcType(RedhatModel redhat, List<NonCharge> nonChargeList, List<BasicPrice> basicPriceList) {
        LocalDate previous = LocalDate.now().minusMonths(1).withDayOfMonth(1);

        var nonCharge = nonChargeList.stream()
                .filter(x -> Objects.equals(x.getNetworkID(), redhat.getNetworkID())
                             && Objects.equals(x.getUuid(), redhat.getVmId())
                             && Objects.equals(x.getOsFlag(), 0)
                             && x.getStartTime().compareTo(previous) <= 0 && x.getStopTime().compareTo(previous) >= 0)
                .findFirst().orElse(null);
        if (nonCharge != null) {
            redhat.setCalcType(Constants.CALC_FREE_TYPE);
        } else {
            redhat.setCalcType(Constants.CALC_BASIC_TYPE);
            var basicPrice = basicPriceList.stream()
                    .filter(x -> Objects.equals(x.getNetworkID(), redhat.getNetworkID()))
                    .findFirst().orElse(null);
            if (basicPrice != null) {
                if (redhat.getVCpu() <= 8) {
                    redhat.setPrice(basicPrice.getUnitPriceRedHat1to8());
                    redhat.setCount(redhat.getVCpu());
                } else if (redhat.getVCpu() <= 127) {
                    redhat.setPrice(basicPrice.getUnitPriceRedHat9to127());
                    redhat.setCount(redhat.getVCpu());
                } else if (redhat.getVCpu() == 128) {
                    redhat.setPrice(basicPrice.getUnitPriceRedHat128());
                    redhat.setCount(redhat.getVCpu());
                } else {
                    redhat.setIsValid(false);
                    redhat.setInvalidReason("vCpu in valid range");
                }
            } else {
                // Trường hợp không tìm thấy basic price thì đánh dấu là không hợp lệ
                redhat.setIsValid(false);
                redhat.setInvalidReason("Not found basic price");
            }
        }
    }

    private void writeToRedhatFile(List<RedhatModel> list, String filename) {
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + File.separator + filename, CharsetUtil.CHARSET_UTF_8);
        List<String[]> dataWriteToFile = list.stream()
                .map(x -> new String[]{
                        x.getNetworkID(),
                        x.getVmId(),
                        x.getVmName(),
                        x.getInstanceName(),
                        x.getDomain(),
                        x.getGuestOsId(),
                        x.getTemplateUuid(),
                        defaultNullIfEmpty(x.getVCpu()),
                        defaultNullIfEmpty(x.getCalcType()),
                        defaultNullIfEmpty(x.getPrice()),
                        defaultNullIfEmpty(x.getCount()),
                        String.valueOf(x.getIsValid()),
                        x.getInvalidReason()
                })
                .collect(Collectors.toList());
        dataWriteToFile.add(0, new String[]{
                "networkID",
                "vmId",
                "vmName",
                "instanceName",
                "domain",
                "guestOsId",
                "templateUuid",
                "vCpu",
                "calcType",
                "price",
                "count",
                "isValid",
                "invalidReason"
        });
        writer.write(dataWriteToFile);
    }
}
