package net.khaibq.addon;

import net.khaibq.addon.service.MasterService;
import net.khaibq.addon.service.MasterServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AiosMaster {
    private static final Logger logger = LogManager.getLogger(AiosMaster.class);

    public static void main(String[] args) {

        MasterService masterService = new MasterServiceImpl();

        logger.info("===== Start clear all data master =====");
        masterService.clearAllDataBasicPrice();
        masterService.clearAllDataRelativePrice();
        masterService.clearAllDataNonCharge();
        logger.info("===== End clear all data master =====");

        logger.info("===== Start call api to insert data master =====");
        masterService.retrieveDataBasicPrice();
        masterService.retrieveDataRelativePrice();
        masterService.retrieveDataNonCharge();
        logger.info("===== End call api to insert data master =====");
    }
}
