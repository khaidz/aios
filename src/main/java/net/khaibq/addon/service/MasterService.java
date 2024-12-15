package net.khaibq.addon.service;

import net.khaibq.addon.model.BasicPrice;
import net.khaibq.addon.model.NonCharge;
import net.khaibq.addon.model.RelativePrice;

import java.util.List;

public interface MasterService {
    void retrieveDataBasicPrice();
    void retrieveDataRelativePrice();
    void retrieveDataNonCharge();
    void insertDBDataBasicPrice(List<BasicPrice> list);
    void insertDBDataRelativePrice(List<RelativePrice> list);
    void insertDBDataNonCharge(List<NonCharge> list);
    void clearAllDataBasicPrice();
    void clearAllDataRelativePrice();
    void clearAllDataNonCharge();

    boolean isValidMasterData();
    List<BasicPrice> getAllBasicPrice();
    List<RelativePrice> getAllRelativePrice();
    List<NonCharge> getAllNonCharge();
}
