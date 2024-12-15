package net.khaibq.addon;

import net.khaibq.addon.service.VirtualMachineService;
import net.khaibq.addon.service.VirtualMachineServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static net.khaibq.addon.utils.Constants.SERVICE_OFFERING_DIR;
import static net.khaibq.addon.utils.Constants.VIRTUAL_MACHINE_DIR;

public class AddonReadFile {
    private static final Logger logger = LogManager.getLogger(AddonReadFile.class);

    public static void main(String[] args) {
        VirtualMachineService virtualMachineService = new VirtualMachineServiceImpl();

        logger.info("===== Start read data Virtual Machine =====");
        virtualMachineService.clearAllDataVirtualMachine();
        virtualMachineService.readVMFile(VIRTUAL_MACHINE_DIR);
        logger.info("===== End read data Virtual Machine =====");

        logger.info("===== Start read data Service Offering =====");
        virtualMachineService.clearAllDataServiceOffering();
        virtualMachineService.readServiceOfferingFile(SERVICE_OFFERING_DIR);
        logger.info("===== end read data Service Offering =====");
    }
}
