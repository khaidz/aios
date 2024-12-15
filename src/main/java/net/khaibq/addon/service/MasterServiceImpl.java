package net.khaibq.addon.service;

import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import net.khaibq.addon.model.BasicPrice;
import net.khaibq.addon.model.NonCharge;
import net.khaibq.addon.model.RelativePrice;
import net.khaibq.addon.utils.Constants;
import net.khaibq.addon.utils.DatabaseConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static net.khaibq.addon.utils.CommonUtils.getInteger;
import static net.khaibq.addon.utils.CommonUtils.getLocalDate;
import static net.khaibq.addon.utils.CommonUtils.getLong;
import static net.khaibq.addon.utils.CommonUtils.getString;

public class MasterServiceImpl implements MasterService {
    private static final Logger logger = LogManager.getLogger(MasterServiceImpl.class);

    @Override
    public void retrieveDataBasicPrice() {
        try {
            var responseBody = callApiGetMasterData(Constants.BASIC_PRICE_DB_SCHEMA_ID);
            CsvReader reader = CsvUtil.getReader();
            CsvData data = reader.readFromStr(responseBody);
            List<CsvRow> rows = data.getRows();
            List<BasicPrice> list = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                BasicPrice basicPrice = new BasicPrice();
                basicPrice.setNetworkID(row.get(0));
                basicPrice.setUnitPriceVM1Cpu(getInteger(row.get(1)));
                basicPrice.setUnitPriceVM1Gb(getInteger(row.get(2)));
                basicPrice.setUnitPriceVDClass1(getInteger(row.get(3)));
                basicPrice.setUnitPriceVDClass2(getInteger(row.get(4)));
                basicPrice.setUnitPriceWindowServer(getInteger(row.get(5)));
                basicPrice.setUnitPriceRedHat1to8(getInteger(row.get(6)));
                basicPrice.setUnitPriceRedHat9to127(getInteger(row.get(7)));
                basicPrice.setUnitPriceRedHat128(getInteger(row.get(7)));
                list.add(basicPrice);
            }
            insertDBDataBasicPrice(list);
        } catch (Exception ex) {
            logger.info("retrieveDataBasicPrice error: {}", ex.getMessage());
        }
    }

    @Override
    public void retrieveDataRelativePrice() {
        try {
            var responseBody = callApiGetMasterData(Constants.RELATIVE_PRICE_DB_SCHEMA_ID);
            CsvReader reader = CsvUtil.getReader();
            CsvData data = reader.readFromStr(responseBody);
            List<CsvRow> rows = data.getRows();
            List<RelativePrice> list = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                RelativePrice relativePrice = new RelativePrice();
                relativePrice.setNetworkID(row.get(0));
                relativePrice.setType(row.get(1));
                relativePrice.setUnitPriceVM1Cpu(getInteger(row.get(2)));
                relativePrice.setUnitPriceVM1Gb(getInteger(row.get(3)));
                relativePrice.setDiskType(row.get(4));
                relativePrice.setDiskSize(getInteger(row.get(5)));
                relativePrice.setMonthPrice(getInteger(row.get(6)));
                relativePrice.setDayPrice(getInteger(row.get(7)));
                relativePrice.setPauseFee(getInteger(row.get(8)));
                list.add(relativePrice);
            }
            insertDBDataRelativePrice(list);
        } catch (Exception ex) {
            logger.info("retrieveDataRelativePrice error: {}", ex.getMessage());
        }

    }

    @Override
    public void retrieveDataNonCharge() {
        try {
            var responseBody = callApiGetMasterData(Constants.NON_CHARGE_DB_SCHEMA_ID);
            CsvReader reader = CsvUtil.getReader();
            CsvData data = reader.readFromStr(responseBody);
            List<CsvRow> rows = data.getRows();
            List<NonCharge> list = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                if (i == 0) continue;
                var row = rows.get(i);
                NonCharge nonCharge = new NonCharge();
                nonCharge.setNetworkID(row.get(0));
                nonCharge.setType(row.get(1));
                nonCharge.setUuid(row.get(2));
                nonCharge.setOsFlag(getInteger(row.get(3)));
                nonCharge.setStartTime(getLocalDate(row.get(4)));
                nonCharge.setStopTime(getLocalDate(row.get(5)));
                list.add(nonCharge);
            }

            insertDBDataNonCharge(list);
        } catch (Exception ex) {
            logger.info("retrieveDataNonCharge error: {}", ex.getMessage());
        }
    }

    private String callApiGetMasterData(String dbSchemaId) {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-HD-apitoken", Constants.X_HD_API_TOKEN);

        Map<String, Object> payload = new HashMap<>();
        payload.put("dbSchemaId", dbSchemaId);
        payload.put("limit", Constants.LIMIT);

        HttpRequest httpRequest = HttpUtil.createPost(Constants.API_GET_DATA_MASTER_URL)
                .contentType("application/json")
                .addHeaders(headers)
                .body(JSONUtil.toJsonStr(payload));
        var response = httpRequest.execute();
        if (response.isOk()) {
            return response.body();
        } else {
            logger.info("Call api get master data error: {}", response.body());
            throw new RuntimeException("Call api get master data error: " + response.body());
        }
    }

    @Override
    public void insertDBDataBasicPrice(List<BasicPrice> list) {
        String sql = """
                insert into aios_master_basic_price(network_id , unit_price_vm_1_cpu, unit_price_vm_1_gb, 
                                     unit_price_vdclass_1, unit_price_vdclass_2, unit_price_window_server, 
                                     unit_price_red_hat_1_to_8, unit_price_red_hat_9_to_127, unit_price_red_hat_128, status)
                values (?,?,?,?,?,?,?,?,?,?);
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < list.size(); i++) {
                BasicPrice item = list.get(i);
                item.setStatus(1);

                // Kiểm tra nết có trường nào null thì đặt status=0 (không hợp lệ)
                if (item.getNetworkID() == null || item.getUnitPriceVM1Cpu() == null || item.getUnitPriceVM1Gb() == null ||
                    item.getUnitPriceVDClass1() == null || item.getUnitPriceVDClass2() == null ||
                    item.getUnitPriceWindowServer() == null || item.getUnitPriceRedHat1to8() == null ||
                    item.getUnitPriceRedHat9to127() == null || item.getUnitPriceRedHat128() == null) {
                    item.setStatus(0);
                }

                ps.setString(1, item.getNetworkID());
                ps.setObject(2, item.getUnitPriceVM1Cpu());
                ps.setObject(3, item.getUnitPriceVM1Gb());
                ps.setObject(4, item.getUnitPriceVDClass1());
                ps.setObject(5, item.getUnitPriceVDClass2());
                ps.setObject(6, item.getUnitPriceWindowServer());
                ps.setObject(7, item.getUnitPriceRedHat1to8());
                ps.setObject(8, item.getUnitPriceRedHat9to127());
                ps.setObject(9, item.getUnitPriceRedHat128());
                ps.setInt(10, item.getStatus());
                ps.addBatch();

                if (i % 20 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== insertDBDataBasicPrice exception: {}", e.getMessage());
        }
    }

    @Override
    public void insertDBDataRelativePrice(List<RelativePrice> list) {
        String sql = """
                insert into aios_master_relative_price(network_id , type, unit_price_vm_1_cpu, unit_price_vm_1_gb,
                                     disk_type, disk_size, month_price, day_price, pause_fee, status)
                values (?,?,?,?,?,?,?,?,?,?);
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < list.size(); i++) {
                RelativePrice item = list.get(i);
                item.setStatus(1);

                //Kiểm tra nết có trường nào null thì đặt status=0 (không hợp lệ)
                if (Objects.equals("仮想サーバ", item.getType())) {
                    if (item.getNetworkID() == null || item.getUnitPriceVM1Cpu() == null || item.getUnitPriceVM1Gb() == null ||
                        item.getMonthPrice() == null || item.getDayPrice() == null || item.getPauseFee() == null) {
                        item.setStatus(0);
                    }
                } else if (Objects.equals("仮想ディスク", item.getType())) {
                    if (item.getDiskType() == null || item.getDiskSize() == null || item.getUnitPriceVM1Gb() == null ||
                        item.getMonthPrice() == null || item.getDayPrice() == null || item.getPauseFee() == null) {
                        item.setStatus(0);
                    }
                }

                ps.setString(1, item.getNetworkID());
                ps.setString(2, item.getType());
                ps.setObject(3, item.getUnitPriceVM1Cpu());
                ps.setObject(4, item.getUnitPriceVM1Gb());
                ps.setString(5, item.getDiskType());
                ps.setObject(6, item.getDiskSize());
                ps.setObject(7, item.getMonthPrice());
                ps.setObject(8, item.getDayPrice());
                ps.setObject(9, item.getPauseFee());
                ps.setObject(10, item.getStatus());
                ps.addBatch();

                if (i % 20 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== insertDBDataRelativePrice exception: {}", e.getMessage());
        }
    }

    @Override
    public void insertDBDataNonCharge(List<NonCharge> list) {
        String sql = """
                insert into aios_master_non_charge(network_id , type, uuid, os_flag, start_time, stop_time, status)
                values (?,?,?,?,?,?,?);
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < list.size(); i++) {
                NonCharge item = list.get(i);
                item.setStatus(1);

                if (item.getNetworkID() == null || item.getType() == null || item.getUuid() == null ||
                    item.getOsFlag() == null || item.getStartTime() == null || item.getStopTime() == null) {
                    item.setStatus(0);
                }

                ps.setString(1, item.getNetworkID());
                ps.setString(2, item.getType());
                ps.setString(3, item.getUuid());
                ps.setObject(4, item.getOsFlag());
                ps.setObject(5, item.getStartTime());
                ps.setObject(6, item.getStopTime());
                ps.setObject(7, item.getStatus());
                ps.addBatch();

                if (i % 20 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== insertDBDataNonCharge exception: {}", e.getMessage());
        }
    }

    @Override
    public void clearAllDataBasicPrice() {
        String sql = "delete from aios.aios_master_basic_price";
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== clearAllDataBasicPrice exception: {}", e.getMessage());
        }
    }

    @Override
    public void clearAllDataRelativePrice() {
        String sql = "delete from aios.aios_master_relative_price";
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== clearAllDataRelativePrice exception: {}", e.getMessage());
        }
    }

    @Override
    public void clearAllDataNonCharge() {
        String sql = "delete from aios.aios_master_non_charge";
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== clearAllDataNonCharge exception: {}", e.getMessage());
        }
    }

    @Override
    public boolean isValidMasterData() {
        String sql = """
                    select (select count(*) from aios_master_basic_price where status != 1) +
                    (select count(*) from aios_master_relative_price where status != 1) +
                    (select count(*) from aios_master_non_charge where status != 1) = 0 is_valid
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (rs.getObject("is_valid") != null) {
                    return rs.getBoolean("is_valid");
                }
            }
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== isValidMasterData exception: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<BasicPrice> getAllBasicPrice() {
        String sql = """
                    select id, network_id, unit_price_vm_1_cpu, unit_price_vm_1_gb,
                    unit_price_vdclass_1, unit_price_vdclass_2, unit_price_window_server,
                    unit_price_red_hat_1_to_8, unit_price_red_hat_9_to_127, unit_price_red_hat_128, 
                    status, created_date from aios_master_basic_price
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            List<BasicPrice> list = new ArrayList<>();
            while (rs.next()) {
                BasicPrice basicPrice = new BasicPrice();
                basicPrice.setId(getLong(rs, "id"));
                basicPrice.setNetworkID(getString(rs, "network_id"));
                basicPrice.setUnitPriceVM1Cpu(getInteger(rs, "unit_price_vm_1_cpu"));
                basicPrice.setUnitPriceVM1Gb(getInteger(rs, "unit_price_vm_1_gb"));
                basicPrice.setUnitPriceVDClass1(getInteger(rs, "unit_price_vdclass_1"));
                basicPrice.setUnitPriceVDClass2(getInteger(rs, "unit_price_vdclass_2"));
                basicPrice.setUnitPriceWindowServer(getInteger(rs, "unit_price_window_server"));
                basicPrice.setUnitPriceRedHat1to8(getInteger(rs, "unit_price_red_hat_1_to_8"));
                basicPrice.setUnitPriceRedHat9to127(getInteger(rs, "unit_price_red_hat_9_to_127"));
                basicPrice.setUnitPriceRedHat128(getInteger(rs, "unit_price_red_hat_128"));
                basicPrice.setStatus(getInteger(rs, "status"));
                list.add(basicPrice);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== getAllBasicPrice exception: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<RelativePrice> getAllRelativePrice() {
        String sql = """
                    select id, network_id, type, unit_price_vm_1_cpu, unit_price_vm_1_gb, 
                    disk_type, disk_size, month_price, day_price, pause_fee, status, created_date 
                    from aios_master_relative_price
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            List<RelativePrice> list = new ArrayList<>();
            while (rs.next()) {
                RelativePrice relativePrice = new RelativePrice();
                relativePrice.setId(getLong(rs, "id"));
                relativePrice.setNetworkID(getString(rs, "network_id"));
                relativePrice.setType(getString(rs, "type"));
                relativePrice.setUnitPriceVM1Cpu(getInteger(rs, "unit_price_vm_1_cpu"));
                relativePrice.setUnitPriceVM1Gb(getInteger(rs, "unit_price_vm_1_gb"));
                relativePrice.setDiskType(getString(rs, "disk_type"));
                relativePrice.setDiskSize(getInteger(rs, "disk_size"));
                relativePrice.setMonthPrice(getInteger(rs, "month_price"));
                relativePrice.setDayPrice(getInteger(rs, "day_price"));
                relativePrice.setPauseFee(getInteger(rs, "pause_fee"));
                relativePrice.setStatus(getInteger(rs, "status"));
                list.add(relativePrice);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== getAllRelativePrice exception: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<NonCharge> getAllNonCharge() {
        String sql = """
                    select id, network_id, type, uuid, os_flag, start_time, stop_time, status, created_date 
                    from aios_master_non_charge
                """;
        try (Connection conn = DatabaseConnection.getInstance(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            List<NonCharge> list = new ArrayList<>();
            while (rs.next()) {
                NonCharge nonCharge = new NonCharge();
                nonCharge.setId(getLong(rs, "id"));
                nonCharge.setNetworkID(getString(rs, "network_id"));
                nonCharge.setType(getString(rs, "type"));
                nonCharge.setUuid(getString(rs, "uuid"));
                nonCharge.setOsFlag(getInteger(rs, "os_flag"));
                nonCharge.setStartTime(getLocalDate(rs, "start_time"));
                nonCharge.setStopTime(getLocalDate(rs, "stop_time"));
                nonCharge.setStatus(getInteger(rs, "status"));
                list.add(nonCharge);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("===== getAllNonCharge exception: {}", e.getMessage());
            return new ArrayList<>();
        }
    }
}
