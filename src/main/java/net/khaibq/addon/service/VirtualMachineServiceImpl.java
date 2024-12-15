package net.khaibq.addon.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import net.khaibq.addon.model.ServiceOfferingModel;
import net.khaibq.addon.model.VirtualMachineModel;
import net.khaibq.addon.model.VirtualMachineCalc;
import net.khaibq.addon.utils.DatabaseConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static net.khaibq.addon.utils.CommonUtils.getDouble;
import static net.khaibq.addon.utils.CommonUtils.getInteger;
import static net.khaibq.addon.utils.CommonUtils.getLong;
import static net.khaibq.addon.utils.CommonUtils.getString;

public class VirtualMachineServiceImpl implements VirtualMachineService {
    private static final Logger logger = LogManager.getLogger(VirtualMachineServiceImpl.class);

    @Override
    public void readVMFile(String path) {
        var listFileName = FileUtil.listFileNames(path);
        var listFileAllocatedVM = listFileName.stream()
                .map(String::toLowerCase)
                .filter(x -> x.contains("ccs") || x.contains("ien"))
                .filter(x -> x.contains("_allocated_vm"))
                .toList();

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

            insertDBDataVirtualMachine(listCombined);
        }
    }

    @Override
    public void clearAllDataVirtualMachine() {
        String sql = "delete from aios.aios_virtual_machine";
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== clearAllDataVirtualMachine exception: {}", e.getMessage());
        }
    }

    @Override
    public void readServiceOfferingFile(String path) {
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

        insertDBDataServiceOffering(list);
    }

    @Override
    public void clearAllDataServiceOffering() {
        String sql = "delete from aios.aios_service_offering";
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== clearAllDataServiceOffering exception: {}", e.getMessage());
        }
    }

    @Override
    public List<VirtualMachineCalc> getAllVirtualMachineCalc() {
        String sql = """
                    select network_id, offer_id, offer_name, vm_id, vm_name, 
                    occupancy_time, COALESCE(vm.running_time, 0) as running_time,
                    (vm.occupancy_time - COALESCE(vm.running_time, 0)) pause_time,
                    so.cpu, so.ram_size,
                    COALESCE(vm.running_time, 0) div 24 running_time_day,
                   (vm.occupancy_time - COALESCE(vm.running_time, 0)) div 24 pause_time_day
                    from aios_virtual_machine vm
                    left join aios_service_offering so on vm.offer_id = so.uuid
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            List<VirtualMachineCalc> list = new ArrayList<>();
            while (rs.next()) {
                VirtualMachineCalc virtualMachine = new VirtualMachineCalc();
                virtualMachine.setNetworkID(getString(rs, "network_id"));
                virtualMachine.setOfferId(getString(rs, "offer_id"));
                virtualMachine.setOfferName(getString(rs, "offer_name"));
                virtualMachine.setVmId(getString(rs, "vm_id"));
                virtualMachine.setVmName(getString(rs, "vm_name"));
                virtualMachine.setOccupancyTime(getDouble(rs, "occupancy_time"));
                virtualMachine.setRunningTime(getDouble(rs, "running_time"));
                virtualMachine.setPauseTime(getDouble(rs, "pause_time"));
                virtualMachine.setRunningTimeDay(getInteger(rs, "running_time_day"));
                virtualMachine.setPauseTimeDay(getInteger(rs, "pause_time_day"));
                virtualMachine.setCpu(getInteger(rs, "cpu"));
                virtualMachine.setRamSize(getInteger(rs, "ram_size"));
                list.add(virtualMachine);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @Override
    public List<ServiceOfferingModel> getAllServiceOffering() {
        String sql = """
                    select id, uuid, domain_path, name, cpu, ram_size, created_date
                    from aios_service_offering
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            List<ServiceOfferingModel> list = new ArrayList<>();
            while (rs.next()) {
                ServiceOfferingModel serviceOfferingModel = new ServiceOfferingModel();
                serviceOfferingModel.setId(getLong(rs, "id"));
                serviceOfferingModel.setUuid(getString(rs, "network_id"));
                serviceOfferingModel.setDomainPath(getString(rs, "domain_path"));
                serviceOfferingModel.setName(getString(rs, "name"));
                serviceOfferingModel.setCpu(getInteger(rs, "cpu"));
                serviceOfferingModel.setRamSize(getInteger(rs, "ram_size"));

                list.add(serviceOfferingModel);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== getAllServiceOffering exception: {}", e.getMessage());
            return new ArrayList<>();
        }
    }


    private static List<VirtualMachineModel> getVmFileData(String type, CsvData data) {
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

    private void insertDBDataVirtualMachine(List<VirtualMachineModel> list) {
        String sql = """
                insert into aios_virtual_machine(filename_date, network_id, offer_id, offer_name, vm_id, vm_name, occupancy_time, running_time)
                values (?,?,?,?,?,?,?,?);
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < list.size(); i++) {
                VirtualMachineModel item = list.get(i);
                ps.setString(1, item.getFilenameDate());
                ps.setString(2, item.getNetworkID());
                ps.setString(3, item.getOfferId());
                ps.setString(4, item.getOfferName());
                ps.setString(5, item.getVmId());
                ps.setString(6, item.getVmName());
                ps.setObject(7, item.getOccupancyTime());
                ps.setObject(8, item.getRunningTime());
                ps.addBatch();

                if (i % 20 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            logger.info("===== insertDBDataVirtualMachine exception: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void insertDBDataServiceOffering(List<ServiceOfferingModel> list) {
        String sql = "insert into aios_service_offering(uuid, domain_path, name, cpu, ram_size) values (?,?,?,?,?)";
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < list.size(); i++) {
                ServiceOfferingModel item = list.get(i);
                ps.setString(1, item.getUuid());
                ps.setString(2, item.getDomainPath());
                ps.setString(3, item.getName());
                ps.setObject(4, item.getCpu());
                ps.setObject(5, item.getRamSize());
                ps.addBatch();

                if (i % 20 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== insertDBDataServiceOffering exception: {}", e.getMessage());
        }
    }

}
