package net.khaibq.addon.service;

import net.khaibq.addon.model.ServiceOfferingModel;
import net.khaibq.addon.model.VirtualMachineCalc;

import java.util.List;

public interface VirtualMachineService {
    void readVMFile(String path);

    void clearAllDataVirtualMachine();

    void readServiceOfferingFile(String path);

    void clearAllDataServiceOffering();

    List<VirtualMachineCalc> getAllVirtualMachineCalc();

    List<ServiceOfferingModel> getAllServiceOffering();
}
