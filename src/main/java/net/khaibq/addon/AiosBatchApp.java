package net.khaibq.addon;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import net.khaibq.addon.model.Output;
import net.khaibq.addon.service.DiskServiceImpl;
import net.khaibq.addon.service.RedhatServiceImpl;
import net.khaibq.addon.service.VirtualMachineServiceImpl;
import net.khaibq.addon.service.WindowServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static net.khaibq.addon.utils.CommonUtils.writeToOutputFile;
import static net.khaibq.addon.utils.Constants.OUTPUT_DIR;

public class AiosBatchApp {
    private static final Logger logger = LogManager.getLogger(AiosBatchApp.class);

    public static void main(String[] args) {
        new VirtualMachineServiceImpl().execute();
        new DiskServiceImpl().execute();
        new RedhatServiceImpl().execute();
        new WindowServiceImpl().execute();

        // Xử lý gộp các file output
        List<Output> list = combinedOutput();
        writeToOutputFile(list, "output.csv");
        System.out.println(list);
    }

    public static List<Output> combinedOutput() {
        var listFileName = List.of("/vm/output-vm.csv", "/disk/output-disk.csv", "/redhat/output-redhat.csv", "/window/output-window.csv");
        List<Output> list = new ArrayList<>();

        for (String fileName : listFileName) {
            CsvReader reader = CsvUtil.getReader();
            CsvData csvData = reader.read(FileUtil.file(OUTPUT_DIR + fileName));
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
        }
        return list;
    }
}
