import time
import logging
import gpiod
from gpiod.line import Direction, Value  # ← Crucial imports for v2 API

log = logging.getLogger()

class GPIORelay:
    def trigger(self):
        log.info(f"Trigger relay for 0.1s in GPIO 17")
        # Raspberry Pi 5 maps the physical 40-pin header to 'gpiochip4'
        chip_path = "/dev/gpiochip4"
        pin_number = 17
        
        # Configure LineSettings using the explicitly imported modules
        line_config = {
            pin_number: gpiod.LineSettings(
                direction=Direction.OUTPUT,
                output_value=Value.INACTIVE  # Start cleanly at LOW
            )
        }
        
        # Request control of the line context securely
        with gpiod.request_lines(
            chip_path,
            consumer="gpio-service-trigger",
            config=line_config
        ) as request:
            
            # Drive pin HIGH (3.3V)
            request.set_values({pin_number: Value.ACTIVE})
            time.sleep(0.1)
            
            # Drop pin back to LOW (0V)
            request.set_values({pin_number: Value.INACTIVE})
        log.info(f"Trigger relay success")

if __name__ == "__main__":
    print("Triggering Pin 17 via native gpiod...")
    GPIORelay().trigger()
    print("Trigger sequence complete.")