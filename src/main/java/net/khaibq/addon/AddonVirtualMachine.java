package net.khaibq.addon;

import cn.hutool.core.lang.Tuple;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import net.khaibq.addon.model.BasicPrice;
import net.khaibq.addon.model.NonCharge;
import net.khaibq.addon.model.Output;
import net.khaibq.addon.model.RelativePrice;
import net.khaibq.addon.model.VirtualMachineCalc;
import net.khaibq.addon.service.MasterService;
import net.khaibq.addon.service.MasterServiceImpl;
import net.khaibq.addon.service.VirtualMachineService;
import net.khaibq.addon.service.VirtualMachineServiceImpl;
import net.khaibq.addon.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.khaibq.addon.utils.CommonUtils.defaultNullIfEmpty;
import static net.khaibq.addon.utils.CommonUtils.writeToOutputFile;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;

public class AddonVirtualMachine {
    private static final Logger logger = LogManager.getLogger(AddonVirtualMachine.class);

    public static void main(String[] args) {
        logger.info("===== Start process Virtual Machine =====");
        MasterService masterService = new MasterServiceImpl();
        VirtualMachineService virtualMachineService = new VirtualMachineServiceImpl();

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
            List<VirtualMachineCalc> originalList = virtualMachineService.getAllVirtualMachineCalc();

            // Xử lý logic tính toán các trường còn thiếu: cpu, ramSize, monthlyPrice, dailyPrice, stopFee, isValid, calcType
            List<VirtualMachineCalc> transformListAll = originalList.stream().map(vm -> {
                // Xử lý và đánh dấu cpu, ram size hợp lệ
                handleValidCpuAndRamSizeVm(vm);

                // Kiểm tra vm là noncharge hay relative -> set các giá trị monthlyPrice, dailyPrice, stopFee tương ứng
                handleCalcType(vm, nonChargeList, relativePriceList, basicPriceList);

                return vm;
            }).toList();

            List<VirtualMachineCalc> transformListNonCharge = transformListAll.stream()
                    .filter(x -> Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Và loại bỏ những vm free
            List<VirtualMachineCalc> transformList = transformListAll.stream()
                    .filter(x -> !Objects.equals(x.getCalcType(), Constants.CALC_FREE_TYPE)).toList();

            // Danh sách các dòng vm không hợp lệ
            List<VirtualMachineCalc> invalidList = transformList.stream().filter(x -> !x.getIsValid()).collect(Collectors.toList());

            // Danh sách các dòng vm hợp lệ
            List<VirtualMachineCalc> validList = transformList.stream().filter(VirtualMachineCalc::getIsValid).toList();

            // Lọc lại 1 lần nữa các networkId có trong invalidList và validList -> không tính toán các networkId này
            List<String> invalidNetworkIds = invalidList.stream().map(VirtualMachineCalc::getNetworkID).distinct().toList();
            invalidList.addAll(validList.stream().filter(x -> invalidNetworkIds.contains(x.getNetworkID())).toList());

            List<VirtualMachineCalc> finalValidList = validList.stream()
                    .filter(x -> !invalidNetworkIds.contains(x.getNetworkID())).collect(Collectors.toList());

            // Tính toán Tổng số tiền theo ngày và phí dừng và xác đinh cờ tính tiền theo tháng
            finalValidList = handleFlagMonth(finalValidList);


            // Nhóm các vm theo networkId, cpu. ram
            Map<Tuple, List<VirtualMachineCalc>> groupNetworkCpuRam = finalValidList.stream()
                    .collect(Collectors.groupingBy(vm -> new Tuple(vm.getNetworkID(), vm.getCpu(), vm.getRamSize() / 1024)));

            List<Output> outputList = new ArrayList<>();
            groupNetworkCpuRam.keySet()
                    .forEach(key -> {
                        List<VirtualMachineCalc> list = groupNetworkCpuRam.get(key);
                        List<VirtualMachineCalc> listFlagMonth1 = list.stream()
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


                        List<VirtualMachineCalc> listFlagMonth0 = list.stream()
                                .filter(x -> x.getFlagMonth() == 0).toList();
                        if (!listFlagMonth0.isEmpty()) {
                            var totalRunningTime = listFlagMonth0.stream()
                                    .map(VirtualMachineCalc::getRunningTime).reduce(0D, Double::sum);

                            Output output = new Output();
                            output.setNetworkID(key.get(0));
                            String plan = key.get(1) + "core-" + key.get(2) + "GB(日単位料金）起動時";
                            output.setPlan(plan);
                            output.setCount((int) Math.round(Math.floor(totalRunningTime / 24)));
                            output.setPrice(listFlagMonth0.get(0).getDailyPrice());
                            outputList.add(output);

                            var totalStopTime = listFlagMonth0.stream()
                                    .map(VirtualMachineCalc::getPauseTime).reduce(0D, Double::sum);
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

            writeToOutputFile(finalOutput);

            writeToVMFile(invalidList, "invalid-vm.csv");

            // Thêm danh sách non charge vào cuối
            List<VirtualMachineCalc> combinedValidAndNonCharge = new ArrayList<>();
            combinedValidAndNonCharge.addAll(finalValidList);
            combinedValidAndNonCharge.addAll(transformListNonCharge);
            writeToVMFile(combinedValidAndNonCharge, "valid-vm.csv");
            logger.info("===== End process Virtual Machine =====");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("===== Exception in process Virtual Machine =====");
            logger.info("========== Reason: {}", e.getMessage());
        }
    }

    private static void handleValidCpuAndRamSizeVm(VirtualMachineCalc vm) {
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

    private static void handleCalcType(VirtualMachineCalc vm, List<NonCharge> nonChargeList, List<RelativePrice> relativePriceList, List<BasicPrice> basicPriceList) {
        LocalDate now = LocalDate.now().withDayOfMonth(1);

        var nonCharge = nonChargeList.stream()
                .filter(x -> Objects.equals(x.getNetworkID(), vm.getNetworkID())
                             && "仮想サーバ".equals(x.getType())
                             && Objects.equals(x.getUuid(), vm.getVmId())
                             && x.getStartTime().compareTo(now) <= 0 && x.getStopTime().compareTo(now) >= 0)
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

    private static List<VirtualMachineCalc> handleFlagMonth(List<VirtualMachineCalc> finalValidList) {
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

    private static void writeToVMFile(List<VirtualMachineCalc> list, String filename) {
        CsvWriter writer = CsvUtil.getWriter(OUTPUT_DIR + File.separator + filename, CharsetUtil.CHARSET_UTF_8);
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
        dataWriteToFile.set(0, new String[]{
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
