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
import net.khaibq.addon.model.RelativePrice;
import net.khaibq.addon.model.VolClassModel;
import net.khaibq.addon.model.VolumeModel;
import net.khaibq.addon.model.VolumeS12S34Model;
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

import static net.khaibq.addon.utils.CommonUtils.defaultNullIfEmpty;
import static net.khaibq.addon.utils.CommonUtils.writeToOutputFile;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;
import static net.khaibq.addon.utils.Constants.VOLUMES_S12_S34_DIR;
import static net.khaibq.addon.utils.Constants.VOLUME_DIR;
import static net.khaibq.addon.utils.Constants.VOL_CLASS_DIR;

public class DiskServiceImpl implements BaseService {
    private static final Logger logger = LogManager.getLogger(DiskServiceImpl.class);

    @Override
    public void execute() {
        logger.info("===== Start process Disk =====");
        MasterService masterService = new MasterServiceImpl();

        // Kiểm tra dữ liệu master có hợp lệ hay không. Nếu không hợp lệ, dừng chương trình ngay
        if (!masterService.isValidMasterData()) {
            logger.info("Dữ liệu master data không hợp lệ. Chương trình kết thúc!");
            return;
        }
        try {
            // Lấy ra dữ liệu các bảng master
            List<BasicPrice> basicPriceList = masterService.getAllBasicPrice();
            List<RelativePrice> relativePriceList = masterService.getAllRelativePrice();
            List<NonCharge> nonChargeList = masterService.getAllNonCharge();

            // Đọc dữ liệu các file
            List<VolumeModel> volumeOriginalList = getVolumeData(VOLUME_DIR);
            List<VolumeS12S34Model> volumeS12S34OriginalList = getVolumeS12S34Data(VOLUMES_S12_S34_DIR);
            List<VolClassModel> volClassOriginalList = getVolClassData(VOL_CLASS_DIR);

            List<String> volumeIds = volumeOriginalList.stream().map(VolumeModel::getVolumeID).toList();
            List<VolumeS12S34Model> volumeS12S34UseList = volumeS12S34OriginalList.stream().filter(x -> volumeIds.contains(x.getUuid())).toList();
            List<VolClassModel> volClassUseList = volClassOriginalList.stream().filter(x -> volumeIds.contains(x.getVolumeId())).toList();

            List<VolumeModel> transformListAll = volumeOriginalList.stream().map(vm -> {
                vm.setIsValid(true);

                // Lấy ra giá trị state từ volumeS12S34UseList
                VolumeS12S34Model volumeS12S34 = volumeS12S34UseList.stream()
                        .filter(x -> Objects.equals(vm.getVolumeID(), x.getUuid()))
                        .findFirst().orElse(null);

                if (volumeS12S34 != null) {
                    vm.setState(volumeS12S34.getState());
                } else {
                    vm.setIsValid(false);
                    vm.setInvalidReason("Not found state");
                    return vm;
                }

                // Lấy ra giá trị class từ volClassUseList
                VolClassModel volClass = volClassUseList.stream()
                        .filter(x -> Objects.equals(vm.getVolumeID(), x.getVolumeId()))
                        .findFirst().orElse(null);
                if (volClass != null) {
                    vm.setClazz(volClass.getClazz());
                } else {
                    vm.setClazz("class1");
                }

                // Tính các giá trị  allocatedSizeGB, occupancyTimeDay
                if (vm.getAllocatedSize() != null) {
                    vm.setAllocatedSizeGB((int) (vm.getAllocatedSize() / 1024 / 1024 / 1024));
                } else {
                    vm.setIsValid(false);
                    vm.setInvalidReason("allocatedSize is null");
                    return vm;
                }

                if (vm.getOccupancyTime() != null) {
                    Integer day = (int) Math.round(Math.floor(vm.getOccupancyTime() / 24));
                    vm.setOccupancyTimeDay(day);
                } else {
                    vm.setIsValid(false);
                    vm.setInvalidReason("occupancyTime is null");
                    return vm;
                }

                // Kiểm tra là noncharge hay relative -> set các giá trị monthlyPrice, dailyPrice, stopFee tương ứng
                handleCalcType(vm, nonChargeList, relativePriceList, basicPriceList);

                return vm;
            }).collect(Collectors.toList());

            List<VolumeModel> transformListNonCharge = transformListAll.stream()
                    .filter(x -> Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Và loại bỏ những vm free
            List<VolumeModel> transformList = transformListAll.stream()
                    .filter(x -> !Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Danh sách các dòng vm không hợp lệ
            List<VolumeModel> invalidList = transformList.stream().filter(x -> !x.getIsValid()).collect(Collectors.toList());

            // Danh sách các dòng vm hợp lệ
            List<VolumeModel> validList = transformList.stream().filter(VolumeModel::getIsValid).toList();

            // Lọc lại 1 lần nữa các networkId có trong invalidList và validList -> không tính toán các networkId này
            List<String> invalidNetworkIds = invalidList.stream().map(VolumeModel::getNetworkID).distinct().toList();
            invalidList.addAll(validList.stream().filter(x -> invalidNetworkIds.contains(x.getNetworkID())).toList());

            List<VolumeModel> finalValidList = validList.stream()
                    .filter(x -> !invalidNetworkIds.contains(x.getNetworkID())).collect(Collectors.toList());

            // Tính toán Tổng số tiền theo ngày và phí dừng và xác đinh cờ tính tiền theo tháng
            finalValidList = handleFlagMonth(finalValidList);

            // Nhóm các vm theo network, class, allocated
            Map<Tuple, List<VolumeModel>> groupNetworkClassAllocated = finalValidList.stream()
                    .collect(Collectors.groupingBy(vm -> new Tuple(vm.getNetworkID(), vm.getClazz(), vm.getAllocatedSizeGB())));

            List<Output> outputList = new ArrayList<>();
            groupNetworkClassAllocated.keySet()
                    .forEach(key -> {
                        List<VolumeModel> list = groupNetworkClassAllocated.get(key);
                        List<VolumeModel> listFlagMonth1 = list.stream()
                                .filter(x -> x.getFlagMonth() == 1).toList();
                        if (!listFlagMonth1.isEmpty()) {
                            Output output = new Output();
                            output.setNetworkID(key.get(0));
                            String plan = buildPlan(key.get(1), key.get(2)) + "(月単位料金）";
                            output.setPlan(plan);
                            output.setCount(listFlagMonth1.size());
                            output.setPrice(listFlagMonth1.get(0).getMonthlyPrice());
                            outputList.add(output);
                        }

                        List<VolumeModel> listFlagMonth0 = list.stream()
                                .filter(x -> x.getFlagMonth() == 0).toList();
                        var totalOccupancyTime = listFlagMonth0.stream()
                                .map(VolumeModel::getOccupancyTime).reduce(0D, Double::sum);
                        if (!listFlagMonth0.isEmpty()){
                            Output output = new Output();
                            output.setNetworkID(key.get(0));
                            String plan = buildPlan(key.get(1), key.get(2)) + "(日単位料金）";
                            output.setPlan(plan);
                            output.setCount((int) Math.round(Math.floor(totalOccupancyTime / 24)));
                            output.setPrice(listFlagMonth0.get(0).getDailyPrice());
                            outputList.add(output);
                        }
                    });
            // Loại bỏ những giá trị count = 0 hoặc price = 0
            List<Output> finalOutput = outputList.stream()
                    .filter(x -> x.getCount() != null && x.getCount() > 0 && x.getPrice() != null && x.getPrice() > 0)
                    .toList();

            writeToOutputFile(finalOutput, "/disk/output-disk.csv");

            writeToDiskFile(invalidList, "/disk/invalid-disk.csv");

            //Thêm danh sách non charge vào cuối
            List<VolumeModel> combinedValidAndNonCharge = new ArrayList<>();
            combinedValidAndNonCharge.addAll(finalValidList);
            combinedValidAndNonCharge.addAll(transformListNonCharge);
            writeToDiskFile(combinedValidAndNonCharge, "/disk/valid-disk.csv");
            logger.info("===== End process Disk =====");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("===== Exception in process Disk =====");
            logger.info("========== Reason: {}", e.getMessage());
        }
    }

    private List<VolumeModel> getVolumeData(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileVolume = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("ccs") || x.contains("ien"))
                .filter(x -> x.contains("_volume"))
                .toList();

        List<VolumeModel> list = new ArrayList<>();

        for (String fileNameVolume : listFileVolume) {
            CsvReader reader = CsvUtil.getReader();
            CsvData csvData = reader.read(FileUtil.file(path + File.separator + fileNameVolume));
            List<CsvRow> rows = csvData.getRows();

            var split = fileNameVolume.split("_");
            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                VolumeModel volumeModel = new VolumeModel();
                volumeModel.setFilenameDate(split[0] + "_" + split[1]);
                volumeModel.setNetworkID(split[2]);
                volumeModel.setVolumeID(row.get(0));
                volumeModel.setDiskOfferingID(row.get(1));
                volumeModel.setDiskOfferingName(row.get(2));
                volumeModel.setTemplateID(row.get(3));
                volumeModel.setTemplateName(row.get(4));
                volumeModel.setZoneID(row.get(5));
                volumeModel.setZoneName(row.get(6));
                volumeModel.setAllocatedSize(row.get(7) != null ? Long.parseLong(row.get(7)) : null);
                volumeModel.setOccupancyTime(row.get(8) != null ? Double.parseDouble(row.get(8)) : null);
                list.add(volumeModel);
            }
        }
        return list;
    }

    private List<VolumeS12S34Model> getVolumeS12S34Data(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileVolume = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("volumes_"))
                .toList();

        List<VolumeS12S34Model> list = new ArrayList<>();

        for (String fileNameVolume : listFileVolume) {
            CsvReader reader = CsvUtil.getReader();
            CsvData csvData = reader.read(FileUtil.file(path + File.separator + fileNameVolume));
            List<CsvRow> rows = csvData.getRows();

            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                VolumeS12S34Model volumeModel = new VolumeS12S34Model();
                volumeModel.setId(row.get(0) != null ? Long.parseLong(row.get(0)) : null);
                volumeModel.setAccountId(row.get(1) != null ? Integer.parseInt(row.get(1)) : null);
                volumeModel.setDomainId(row.get(2) != null ? Integer.parseInt(row.get(2)) : null);
                volumeModel.setName(row.get(3));
                volumeModel.setUuid(row.get(4));
                volumeModel.setPoolName(row.get(5));
                volumeModel.setState(row.get(6));
                list.add(volumeModel);
            }
        }
        return list;
    }

    private List<VolClassModel> getVolClassData(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileVolume = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("volclass"))
                .toList();

        List<VolClassModel> list = new ArrayList<>();

        for (String fileNameVolume : listFileVolume) {
            CsvReader reader = CsvUtil.getReader();
            CsvData csvData = reader.read(FileUtil.file(path + File.separator + fileNameVolume));
            List<CsvRow> rows = csvData.getRows();

            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                VolClassModel volClassModel = new VolClassModel();
                volClassModel.setVolumeId(row.get(0));
                volClassModel.setClazz(row.get(1));
                volClassModel.setStorage(row.get(2));
                list.add(volClassModel);
            }
        }
        return list;
    }

    private void handleCalcType(VolumeModel vm, List<NonCharge> nonChargeList, List<RelativePrice> relativePriceList, List<BasicPrice> basicPriceList) {
        LocalDate now = LocalDate.now().withDayOfMonth(1);

        var nonCharge = nonChargeList.stream()
                .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID())
                             && "仮想ディスク".equals(x.getType())
                             && Objects.equals(x.getUuid(), vm.getVolumeID())
                             && (x.getStartTime().compareTo(now) <= 0 && x.getStopTime().compareTo(now) >= 0 || "Destroy".equals(vm.getState())))
                .findFirst().orElse(null);
        if (nonCharge != null) {
            vm.setCalcType(Constants.CALC_FREE_TYPE);
        } else {
            var relativePrice = relativePriceList.stream()
                    .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID())
                                 && "仮想ディスク".equals(x.getType())
                                 && Objects.equals(x.getDiskType(), vm.getClazz())
                                 && Objects.equals(x.getDiskSize(), vm.getAllocatedSizeGB())
                    )
                    .findFirst().orElse(null);
            if (relativePrice != null) {
                vm.setCalcType(Constants.CALC_RELATIVE_TYPE);
                vm.setMonthlyPrice(relativePrice.getMonthPrice());
                vm.setDailyPrice(relativePrice.getDayPrice());
            } else {
                vm.setCalcType(Constants.CALC_BASIC_TYPE);
                var basicPrice = basicPriceList.stream()
                        .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID()))
                        .findFirst().orElse(null);
                if (basicPrice != null) {
                    if (Objects.equals(vm.getClazz(), "class1")) {
                        int monthlyPrice = vm.getAllocatedSizeGB() / 10 * basicPrice.getUnitPriceVDClass1();
                        vm.setMonthlyPrice(monthlyPrice);
                        vm.setDailyPrice(monthlyPrice / 20);
                    } else if (Objects.equals(vm.getClazz(), "class2")) {
                        int monthlyPrice = vm.getAllocatedSizeGB() / 10 * basicPrice.getUnitPriceVDClass2();
                        vm.setMonthlyPrice(monthlyPrice);
                        vm.setDailyPrice(monthlyPrice / 20);
                    } else {
                        vm.setIsValid(false);
                        vm.setInvalidReason("Not found class in basic price");
                    }
                } else {
                    // Trường hợp không tìm thấy basic price thì đánh dấu là không hợp lệ
                    vm.setIsValid(false);
                    vm.setInvalidReason("Not found basic price");
                }
            }
        }
    }

    private List<VolumeModel> handleFlagMonth(List<VolumeModel> finalValidList) {
        return finalValidList.stream().map(x -> {
            if (x.getOccupancyTimeDay() >= 20) {
                x.setFlagMonth(1);
            } else {
                x.setFlagMonth(0);
            }
            return x;
        }).toList();
    }

    private String buildPlan(String clazz, Integer allocatedGB) {
        if (allocatedGB < 1000) {
            return "HDD-" + clazz + "-" + allocatedGB + "GB";
        }
        if (allocatedGB == 1024) {
            return "HDD-" + clazz + "-1TB";
        }
        return "HDD-" + clazz + "-" + String.format("%.2f", (double) allocatedGB / 1000) + "GB";
    }

    private void writeToDiskFile(List<VolumeModel> list, String filename) {
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + File.separator + filename, CharsetUtil.CHARSET_UTF_8);
        List<String[]> dataWriteToFile = list.stream()
                .map(x -> new String[]{
                        x.getFilenameDate(),
                        x.getNetworkID(),
                        x.getVolumeID(),
                        x.getDiskOfferingID(),
                        x.getDiskOfferingName(),
                        x.getTemplateID(),
                        x.getTemplateName(),
                        x.getZoneID(),
                        x.getZoneName(),
                        defaultNullIfEmpty(x.getAllocatedSize()),
                        defaultNullIfEmpty(x.getOccupancyTime()),
                        x.getClazz(),
                        x.getState(),
                        defaultNullIfEmpty(x.getCalcType()),
                        defaultNullIfEmpty(x.getMonthlyPrice()),
                        defaultNullIfEmpty(x.getDailyPrice()),
                        defaultNullIfEmpty(x.getAllocatedSizeGB()),
                        defaultNullIfEmpty(x.getOccupancyTimeDay()),
                        defaultNullIfEmpty(x.getFlagMonth()),
                        String.valueOf(x.getIsValid()),
                        x.getInvalidReason()
                })
                .collect(Collectors.toList());
        dataWriteToFile.add(0, new String[]{
                "filenameDate",
                "networkID",
                "volumeID",
                "diskOfferingID",
                "diskOfferingName",
                "templateID",
                "templateName",
                "zoneID",
                "zoneName",
                "allocatedSize",
                "occupancyTime",
                "clazz",
                "state",
                "calcType",
                "monthlyPrice",
                "dailyPrice",
                "allocatedSizeGB",
                "occupancyTimeDay",
                "flagMonth",
                "isValid"
        });
        writer.write(dataWriteToFile);
    }
}
