import { Codegen } from "codegen";

interface IoTDeviceConfig {
  deviceId: string;
  telemetryTopic: string;
  messageBrokerUrl: string;
}

class IoTDevice {
  private config: IoTDeviceConfig;

  constructor(config: IoTDeviceConfig) {
    this.config = config;
  }

  sendTelemetry(data: any) {
    // Send telemetry data to message broker
    console.log(
      `Sending telemetry data from device ${this.config.deviceId} to topic ${this.config.telemetryTopic}`
    );
  }
}

export { IoTDevice, IoTDeviceConfig };
