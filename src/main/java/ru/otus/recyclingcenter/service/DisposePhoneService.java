package ru.otus.recyclingcenter.service;

import avro.schema.SmartPhoneAvro;
import avro.schema.detail.BatteryAvro;
import avro.schema.detail.MotherBoardAvro;
import avro.schema.detail.ScreenAvro;
import org.springframework.stereotype.Service;

@Service
public class DisposePhoneService {

    public MotherBoardAvro removeMotherBoardFromPhone(SmartPhoneAvro phoneAvro) {
        final var oldMb = phoneAvro.getMotherBoard();
        phoneAvro.setMotherBoard(null);
        return oldMb;
    }

    public BatteryAvro removeBatteryFromPhone(SmartPhoneAvro phoneAvro) {
        final var oldBattery = phoneAvro.getBattery();
        phoneAvro.setBattery(null);
        return oldBattery;
    }

    public ScreenAvro removeScreenFromPhone(SmartPhoneAvro phoneAvro) {
        final var oldScreen = phoneAvro.getScreen();
        phoneAvro.setScreen(null);
        return oldScreen;
    }
}
