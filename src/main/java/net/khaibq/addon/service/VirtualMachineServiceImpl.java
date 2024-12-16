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
import net.khaibq.addon.model.ServiceOfferingModel;
import net.khaibq.addon.model.VirtualMachineModel;
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
import static net.khaibq.addon.utils.Constants.SERVICE_OFFERING_DIR;
import static net.khaibq.addon.utils.Constants.VIRTUAL_MACHINE_DIR;

public class VirtualMachineServiceImpl implements BaseService {
    private static final Logger logger = LogManager.getLogger(VirtualMachineServiceImpl.class);

    @Override
    public void execute() {
        logger.info("===== Start process Virtual Machine =====");
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

            // Lấy ra danh sách máy ảo
            List<VirtualMachineModel> originalList = getVirtualMachineModel(VIRTUAL_MACHINE_DIR);
            List<ServiceOfferingModel> serviceOfferingOriginalList = getServiceOfferingModel(SERVICE_OFFERING_DIR);

            List<String> serviceOfferIds = originalList.stream().map(VirtualMachineModel::getOfferId).collect(Collectors.toList());
            List<ServiceOfferingModel> serviceOfferingUserList = serviceOfferingOriginalList.stream()
                    .filter(x -> serviceOfferIds.contains(x.getUuid()))
                    .toList();

            // Xử lý logic tính toán các trường còn thiếu: cpu, ramSize, monthlyPrice, dailyPrice, stopFee, isValid, calcType
            List<VirtualMachineModel> transformListAll = originalList.stream().map(vm -> {
                ServiceOfferingModel serviceOffering = serviceOfferingUserList.stream()
                        .filter(x -> Objects.equals(vm.getOfferId(), x.getUuid()))
                        .findFirst().orElse(null);
                if (serviceOffering != null) {
                    vm.setCpu(serviceOffering.getCpu());
                    vm.setRamSize(serviceOffering.getRamSize());
                }

                // Xử lý và đánh dấu cpu, ram size hợp lệ
                handleValidCpuAndRamSizeVm(vm);

                // Kiểm tra vm là noncharge hay relative -> set các giá trị monthlyPrice, dailyPrice, stopFee tương ứng
                handleCalcType(vm, nonChargeList, relativePriceList, basicPriceList);

                return vm;
            }).toList();

            List<VirtualMachineModel> transformListNonCharge = transformListAll.stream()
                    .filter(x -> Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Và loại bỏ những vm free
            List<VirtualMachineModel> transformList = transformListAll.stream()
                    .filter(x -> !Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Danh sách các dòng vm không hợp lệ
            List<VirtualMachineModel> invalidList = transformList.stream().filter(x -> !x.getIsValid()).collect(Collectors.toList());

            // Danh sách các dòng vm hợp lệ
            List<VirtualMachineModel> validList = transformList.stream().filter(VirtualMachineModel::getIsValid).toList();

            // Lọc lại 1 lần nữa các networkId có trong invalidList và validList -> không tính toán các networkId này
            List<String> invalidNetworkIds = invalidList.stream().map(VirtualMachineModel::getNetworkID).distinct().toList();
            invalidList.addAll(validList.stream().filter(x -> invalidNetworkIds.contains(x.getNetworkID())).toList());

            List<VirtualMachineModel> finalValidList = validList.stream()
                    .filter(x -> !invalidNetworkIds.contains(x.getNetworkID())).collect(Collectors.toList());

            // Tính toán Tổng số tiền theo ngày và phí dừng và xác đinh cờ tính tiền theo tháng
            finalValidList = handleFlagMonth(finalValidList);


            // Nhóm các vm theo networkId, cpu. ram
            Map<Tuple, List<VirtualMachineModel>> groupNetworkCpuRam = finalValidList.stream()
                    .collect(Collectors.groupingBy(vm -> new Tuple(vm.getNetworkID(), vm.getCpu(), vm.getRamSize() / 1024)));

            List<Output> outputList = new ArrayList<>();
            groupNetworkCpuRam.keySet()
                    .forEach(key -> {
                        List<VirtualMachineModel> list = groupNetworkCpuRam.get(key);
                        List<VirtualMachineModel> listFlagMonth1 = list.stream()
                                .filter(x -> x.getFlagMonth() == 1).toList();
                        if (!listFlagMonth1.isEmpty()) {
                            Output output = new Output();
                            output.setNetworkID(key.get(0));
                            String plan = key.get(1) + "core-" + key.get(2) + "GB(月単位料金）起動時";
                            output.setPlan(plan);
                            output.setCount(listFlagMonth1.size());
                            output.setPrice(listFlagMonth1.get(0).getMonthlyPrice());
                            outputList.add(output);
                        }


                        List<VirtualMachineModel> listFlagMonth0 = list.stream()
                                .filter(x -> x.getFlagMonth() == 0).toList();
                        if (!listFlagMonth0.isEmpty()) {
                            var totalRunningTime = listFlagMonth0.stream()
                                    .map(VirtualMachineModel::getRunningTime).reduce(0D, Double::sum);

                            Output output = new Output();
                            output.setNetworkID(key.get(0));
                            String plan = key.get(1) + "core-" + key.get(2) + "GB(日単位料金）起動時";
                            output.setPlan(plan);
                            output.setCount((int) Math.round(Math.floor(totalRunningTime / 24)));
                            output.setPrice(listFlagMonth0.get(0).getDailyPrice());
                            outputList.add(output);

                            var totalStopTime = listFlagMonth0.stream()
                                    .map(VirtualMachineModel::getPauseTime).reduce(0D, Double::sum);
                            Output output2 = new Output();
                            output2.setNetworkID(key.get(0));
                            String plan2 = key.get(1) + "core-" + key.get(2) + "GB(日単位料金）停止時";
                            output2.setPlan(plan2);
                            output2.setCount((int) Math.round(Math.floor(totalStopTime / 24)));
                            output2.setPrice(listFlagMonth0.get(0).getDailyPrice());
                            outputList.add(output2);
                        }
                    });

            // Loại bỏ những giá trị count = 0 hoặc price = 0
            List<Output> finalOutput = outputList.stream()
                    .filter(x -> x.getCount() != null && x.getCount() > 0 && x.getPrice() != null && x.getPrice() > 0)
                    .toList();

            writeToOutputFile(finalOutput, "/vm/output-vm.csv");

            writeToVMFile(invalidList, "/vm/invalid-vm.csv");

            // Thêm danh sách non charge vào cuối
            List<VirtualMachineModel> combinedValidAndNonCharge = new ArrayList<>();
            combinedValidAndNonCharge.addAll(finalValidList);
            combinedValidAndNonCharge.addAll(transformListNonCharge);
            writeToVMFile(combinedValidAndNonCharge, "/vm/valid-vm.csv");
            logger.info("===== End process Virtual Machine =====");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("===== Exception in process Virtual Machine =====");
            logger.info("========== Reason: {}", e.getMessage());
        }
    }


    private List<VirtualMachineModel> getVirtualMachineModel(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileAllocatedVM = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("ccs") || x.contains("ien"))
                .filter(x -> x.contains("_allocated_vm"))
                .toList();

        List<VirtualMachineModel> list = new ArrayList<>();

        for (String fileNameAllocatedVM : listFileAllocatedVM) {
            CsvReader reader = CsvUtil.getReader();
            CsvData data = reader.read(FileUtil.file(path + File.separator + fileNameAllocatedVM));
            List<VirtualMachineModel> listAllocated = getVmFileData("Allocated", data);

            String fileNameRunningVM = fileNameAllocatedVM.replaceAll("allocated_vm", "running_vm");
            data = reader.read(FileUtil.file(path + File.separator + fileNameRunningVM));
            List<VirtualMachineModel> listRunning = getVmFileData("Running", data);

            List<VirtualMachineModel> listCombined = listAllocated.stream().map(x -> {
                var split = fileNameAllocatedVM.split("_");
                x.setFilenameDate(split[0] + "_" + split[1]);
                x.setNetworkID(split[2]);
                listRunning.stream()
                        .filter(y -> Objects.equals(x.getOfferId(), y.getOfferId()) && Objects.equals(x.getOfferName(), y.getOfferName())
                                     && Objects.equals(x.getVmId(), y.getVmId()) && Objects.equals(x.getVmName(), y.getVmName()))
                        .findFirst()
                        .ifPresent(itemRunning -> x.setRunningTime(itemRunning.getRunningTime()));
                return x;
            }).toList();

            listCombined = listCombined.stream().map(x -> {
                if (x.getRunningTime() == null) {
                    x.setRunningTime(0d);
                }
                x.setPauseTime(x.getOccupancyTime() - x.getRunningTime());
                x.setRunningTimeDay((int) Math.round(Math.floor(x.getRunningTime() / 24)));
                x.setPauseTimeDay((int) Math.round(Math.floor(x.getPauseTime() / 24)));

                // cpu, ram_size
                return x;
            }).toList();

            list.addAll(listCombined);
        }

        return list;
    }

    private List<ServiceOfferingModel> getServiceOfferingModel(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileServiceOffering = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.startsWith("serviceoffering_s"))
                .toList();

        List<ServiceOfferingModel> list = new ArrayList<>();
        for (String fileNameServiceOffering : listFileServiceOffering) {
            CsvReader reader = CsvUtil.getReader();
            CsvData data = reader.read(FileUtil.file(path + File.separator + fileNameServiceOffering));
            List<CsvRow> rows = data.getRows();
            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                ServiceOfferingModel serviceOfferingModel = new ServiceOfferingModel();
                serviceOfferingModel.setUuid(row.get(1));
                serviceOfferingModel.setDomainPath(row.get(2));
                serviceOfferingModel.setName(row.get(3));
                serviceOfferingModel.setCpu(row.get(4) != null ? Integer.parseInt(row.get(4)) : null);
                serviceOfferingModel.setRamSize(row.get(5) != null ? Integer.parseInt(row.get(5)) : null);
                list.add(serviceOfferingModel);
            }
        }
        return list;
    }

    private List<VirtualMachineModel> getVmFileData(String type, CsvData data) {
        List<CsvRow> rows = data.getRows();
        List<VirtualMachineModel> list = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            if (i == 0) continue;
            var row = rows.get(i);
            VirtualMachineModel virtualMachineModel = new VirtualMachineModel();
            virtualMachineModel.setOfferId(row.get(0));
            virtualMachineModel.setOfferName(row.get(1));
            virtualMachineModel.setVmId(row.get(2));
            virtualMachineModel.setVmName(row.get(3));
            if ("Allocated".equals(type)) {
                virtualMachineModel.setOccupancyTime(row.get(4) != null ? Double.parseDouble(row.get(4)) : null);
            } else if ("Running".equals(type)) {
                virtualMachineModel.setRunningTime(row.get(4) != null ? Double.parseDouble(row.get(4)) : null);
            }
            list.add(virtualMachineModel);
        }
        return list;
    }

    private void handleValidCpuAndRamSizeVm(VirtualMachineModel vm) {
        vm.setIsValid(true);
        if (vm.getCpu() == null || vm.getRamSize() == null) {
            if ("V-1core-2GB".equals(vm.getOfferName())) {
                vm.setCpu(1);
                vm.setRamSize(2 * 1024);
            } else if ("V-2core-4GB".equals(vm.getOfferName())) {
                vm.setCpu(2);
                vm.setRamSize(4 * 1024);
            } else if ("V-4core-8GB".equals(vm.getOfferName())) {
                vm.setCpu(4);
                vm.setRamSize(8 * 1024);
            } else {
                vm.setIsValid(false);
                vm.setInvalidReason("Cpu or ram_size empty");
            }
        }
    }

    private void handleCalcType(VirtualMachineModel vm, List<NonCharge> nonChargeList, List<RelativePrice> relativePriceList, List<BasicPrice> basicPriceList) {
        LocalDate previous = LocalDate.now().minusMonths(1).withDayOfMonth(1);

        var nonCharge = nonChargeList.stream()
                .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID())
                             && "仮想サーバ".equals(x.getType())
                             && Objects.equals(x.getUuid(), vm.getVmId())
                             && x.getStartTime().compareTo(previous) <= 0 && x.getStopTime().compareTo(previous) >= 0)
                .findFirst().orElse(null);
        if (nonCharge != null) {
            vm.setCalcType(Constants.CALC_FREE_TYPE);
        } else {
            var relativePrice = relativePriceList.stream()
                    .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID())
                                 && "仮想サーバ".equals(x.getType())
                                 && Objects.equals(x.getUnitPriceVM1Cpu(), vm.getCpu())
                                 && Objects.equals(x.getUnitPriceVM1Gb() * 1024, vm.getRamSize()))
                    .findFirst().orElse(null);
            if (relativePrice != null) {
                vm.setCalcType(Constants.CALC_RELATIVE_TYPE);
                vm.setMonthlyPrice(relativePrice.getMonthPrice());
                vm.setDailyPrice(relativePrice.getDayPrice());
                vm.setStopFee(relativePrice.getPauseFee());
            } else {
                vm.setCalcType(Constants.CALC_BASIC_TYPE);
                var basicPrice = basicPriceList.stream()
                        .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID()))
                        .findFirst().orElse(null);
                if (basicPrice != null) {
                    int monthlyPrice = vm.getCpu() * basicPrice.getUnitPriceVM1Cpu() + vm.getRamSize() / 1024 * basicPrice.getUnitPriceVM1Gb();
                    vm.setMonthlyPrice(monthlyPrice);
                    int dailyPrice = monthlyPrice / 20;
                    vm.setDailyPrice(dailyPrice);
                    vm.setStopFee(dailyPrice / 2);
                } else {
                    // Trường hợp không tìm thấy basic price thì đánh dấu là không hợp lệ
                    vm.setIsValid(false);
                    vm.setInvalidReason("Not found basic price");
                }
            }
        }
    }

    private List<VirtualMachineModel> handleFlagMonth(List<VirtualMachineModel> finalValidList) {
        return finalValidList.stream().map(x -> {
            Integer totalAmount = x.getRunningTimeDay() * x.getDailyPrice() + x.getPauseTimeDay() * x.getStopFee();
            x.setTotalAmount(totalAmount);

            //Nếu số ngày hoạt động >= 20 hoặc totalAmount > Số tiền theo tháng thì tính tiền theo tháng
            if (x.getRunningTimeDay() >= 20 || totalAmount > x.getMonthlyPrice()) {
                x.setFlagMonth(1);
            } else {
                x.setFlagMonth(0);
            }
            return x;
        }).toList();
    }

    private void writeToVMFile(List<VirtualMachineModel> list, String filename) {
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + filename, CharsetUtil.CHARSET_UTF_8);
        List<String[]> dataWriteToFile = list.stream()
                .map(x -> new String[]{
                        x.getNetworkID(),
                        x.getOfferId(),
                        x.getOfferName(),
                        x.getVmId(),
                        x.getVmName(),
                        defaultNullIfEmpty(x.getOccupancyTime()),
                        defaultNullIfEmpty(x.getRunningTime()),
                        defaultNullIfEmpty(x.getCpu()),
                        defaultNullIfEmpty(x.getRamSize()),
                        defaultNullIfEmpty(x.getPauseTime()),
                        defaultNullIfEmpty(x.getRunningTimeDay()),
                        defaultNullIfEmpty(x.getPauseTimeDay()),
                        defaultNullIfEmpty(x.getCalcType()),
                        defaultNullIfEmpty(x.getMonthlyPrice()),
                        defaultNullIfEmpty(x.getDailyPrice()),
                        defaultNullIfEmpty(x.getStopFee()),
                        defaultNullIfEmpty(x.getTotalAmount()),
                        defaultNullIfEmpty(x.getFlagMonth()),
                        String.valueOf(x.getIsValid()),
                        x.getInvalidReason()
                })
                .collect(Collectors.toList());
        dataWriteToFile.add(0, new String[]{
                "networkID",
                "offerId",
                "offerName",
                "vmId",
                "vmName",
                "occupancyTime",
                "runningTime",
                "cpu",
                "ramSize",
                "pauseTime",
                "runningTimeDay",
                "pauseTimeDay",
                "calcType",
                "monthlyPrice",
                "dailyPrice",
                "stopFee",
                "totalAmount",
                "flagMonth",
                "isValid",
                "invalidReason"
        });
        writer.write(dataWriteToFile);
    }
}
