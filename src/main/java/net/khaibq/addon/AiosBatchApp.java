package net.khaibq.addon;

import net.khaibq.addon.service.DiskServiceImpl;
import net.khaibq.addon.service.RedhatServiceImpl;
import net.khaibq.addon.service.VirtualMachineServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AiosBatchApp {
    private static final Logger logger = LogManager.getLogger(AiosBatchApp.class);

    public static void main(String[] args) {
        new VirtualMachineServiceImpl().execute();
        new DiskServiceImpl().execute();
        new RedhatServiceImpl().execute();
//        new WindowServiceImpl().execute();
    }
}
