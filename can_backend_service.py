import can
import cantools
import os
import time
import random
import threading
import zmq
import json # To serialize messages for network transmission
from datetime import datetime

# --- Configuration (can be moved to config file or command line args) ---
DBC_FILE_PATH = 'dbc/Current.dbc' # This backend will manage its own DBC
CAN_BUS_INTERFACE = 'socketcan'
CAN_BUS_CHANNEL = 'vcan0'
CAN_BUS_BITRATE = 500000

# ZeroMQ Ports
PUB_PORT = "5556" # For publishing CAN messages
REQ_REP_PORT = "5557" # For command/response

# --- Global Variables for Backend ---
db = None
bus = None
can_listening_running = False
dummy_sender_thread = None

# ZeroMQ Context and Sockets
context = zmq.Context()
can_pub_socket = context.socket(zmq.PUB)
command_rep_socket = context.socket(zmq.REP)

def load_dbc_file(file_path):
    """Loads a DBC file. Called by frontend command."""
    global db
    if not os.path.exists(file_path):
        print(f"Error: DBC file not found at '{file_path}'")
        return {"status": "error", "message": f"DBC file not found: {file_path}"}
    try:
        db = cantools.database.load_file(file_path)
        print(f"Successfully loaded DBC file: {file_path}")
        return {"status": "success", "message": f"DBC loaded: {file_path}"}
    except Exception as e:
        print(f"Error loading DBC file: {e}")
        return {"status": "error", "message": f"Error loading DBC: {e}"}

def setup_can_bus(interface, channel, bitrate):
    """Sets up the CAN bus. Called by frontend command."""
    global bus, dummy_sender_thread, can_listening_running
    print(f"Attempting to connect to CAN bus: {interface}/{channel} @ {bitrate} bps")
    try:
        if bus:
            bus.shutdown() # Ensure previous bus is shut down

        if interface == 'virtual':
            bus = can.interface.Bus(bustype='virtual', channel='test_channel', bitrate=bitrate)
            print("Using virtual CAN bus for demonstration.")
            if dummy_sender_thread and dummy_sender_thread.is_alive():
                # For more robust stop, would need a flag in dummy sender
                pass
            dummy_sender_thread = threading.Thread(target=_send_dummy_can_messages_thread, daemon=True)
            dummy_sender_thread.start()
        else:
            bus = can.interface.Bus(bustype=interface, channel=channel, bitrate=bitrate)
            print(f"Connected to CAN bus via {interface} on channel {channel}")

        can_listening_running = True # Set flag to allow listener to start
        return {"status": "success", "message": f"CAN bus connected: {interface}/{channel}"}
    except Exception as e:
        print(f"Error setting up CAN bus: {e}")
        can_listening_running = False # Reset flag on failure
        return {"status": "error", "message": f"Error connecting CAN bus: {e}"}

def shutdown_can_bus():
    """Shuts down the CAN bus. Called by frontend command or on exit."""
    global bus, can_listening_running, dummy_sender_thread
    print("Stopping CAN bus listener.")
    can_listening_running = False # Signal the listening loop to stop
    if dummy_sender_thread and dummy_sender_thread.is_alive():
        # A more robust stop for the dummy sender would be needed if it loops indefinitely
        pass

    if bus:
        try:
            bus.shutdown()
            bus = None
            print("CAN bus shut down.")
            return {"status": "success", "message": "CAN bus shut down."}
        except Exception as e:
            print(f"Error during CAN bus shutdown: {e}")
            return {"status": "error", "message": f"Error during CAN bus shutdown: {e}"}
    return {"status": "success", "message": "CAN bus already disconnected."}


def _send_dummy_can_messages_thread():
    """Sends dummy messages for the virtual bus (backend only)."""
    global db, bus, can_listening_running
    if not db:
        print("Dummy sender: No DBC loaded.")
        return

    messages_to_send = []
    for msg_def in db.messages:
         if msg_def.signals:
             messages_to_send.append(msg_def)
             if len(messages_to_send) >= 5:
                 break
    if not messages_to_send:
        print("Dummy sender: No suitable messages found in DBC.")
        return

    print("Dummy CAN message sender started.")
    while can_listening_running and bus: # Use the main listening flag to control loop
        try:
            for msg_def in messages_to_send:
                if not can_listening_running or not bus: break
                data = {}
                for signal in msg_def.signals:
                    if signal.is_float:
                        data[signal.name] = random.uniform(signal.minimum or 0, signal.maximum or 100)
                    else:
                        data[signal.name] = random.randint(int(signal.minimum or 0), int(signal.maximum or 100))
                
                encoded_data = db.encode_message(msg_def.name, data)
                message = can.Message(arbitration_id=msg_def.frame_id, data=encoded_data, is_extended_id=False)
                bus.send(message)
            time.sleep(0.1) # Send every 100ms
        except Exception as e:
            print(f"Dummy sender error: {e}")
            break
    print("Dummy CAN message sender stopped.")


def can_message_listener_thread():
    """Continuously receives, decodes, and publishes CAN bus data via ZeroMQ PUB socket."""
    global db, bus, can_listening_running
    print("CAN message listener thread started.")

    while can_listening_running: # Loop as long as connection is active
        if not db or not bus:
            time.sleep(0.1) # Wait if bus/db not ready
            continue

        try:
            message = bus.recv(timeout=0.1) # Shorter timeout for quicker shutdown responsiveness
            if message:
                timestamp = message.timestamp
                message_id = message.arbitration_id
                message_id_hex = f"0x{message.arbitration_id:X}"

                try:
                    message_definition = db.get_message_by_frame_id(message_id)
                    message_name = message_definition.name
                    decoded_data = db.decode_message(message_id, message.data)

                    # Prepare for JSON serialization
                    serializable_decoded_data = {k: v for k, v in decoded_data.items()}

                    # --- ZeroMQ PUB: Publish decoded message ---
                    can_pub_socket.send_json({
                        "type": "decoded", # Indicate message type
                        "timestamp": timestamp,
                        "id_hex": message_id_hex,
                        "name": message_name,
                        "data": serializable_decoded_data
                    })
                except KeyError:
                    # --- ZeroMQ PUB: Publish raw message if ID not found in DBC ---
                    can_pub_socket.send_json({
                        "type": "raw", # Indicate message type
                        "timestamp": timestamp,
                        "id_hex": message_id_hex,
                        "data_hex": message.data.hex() # Convert bytes to hex string for JSON
                    })
                except Exception as e:
                    print(f"Error decoding message {message_id_hex}: {e}")
                    # Could also publish error messages if a specific channel for them exists
        except Exception as e:
            if can_listening_running:
                print(f"An unexpected error occurred during CAN reception: {e}")
            time.sleep(0.1)

    print("CAN message listener thread stopped.")

def handle_commands():
    """Handles incoming commands from the frontend via ZeroMQ REP socket."""
    while True:
        try:
            # --- ZeroMQ REP: Receive command from frontend ---
            message = command_rep_socket.recv_json()
            print(f"Received command: {message}")
            command = message.get("command")
            args = message.get("args", {})

            response = {"status": "error", "message": "Unknown command"}

            if command == "load_dbc":
                response = load_dbc_file(args.get("file_path"))
            elif command == "connect_can":
                response = setup_can_bus(
                    args.get("interface"),
                    args.get("channel"),
                    args.get("bitrate")
                )
            elif command == "disconnect_can":
                response = shutdown_can_bus()
            elif command == "send_can_message":
                msg_name = args.get("message_name")
                signal_data = args.get("signal_data")
                if not db:
                    response = {"status": "error", "message": "DBC not loaded. Cannot send message."}
                elif not bus:
                    response = {"status": "error", "message": "CAN bus not connected. Cannot send message."}
                elif not msg_name or not signal_data:
                    response = {"status": "error", "message": "Missing message_name or signal_data."}
                else:
                    try:
                        msg_def = db.get_message_by_name(msg_name)
                        encoded_data = db.encode_message(msg_def.name, signal_data)
                        message = can.Message(arbitration_id=msg_def.frame_id, data=encoded_data, is_extended_id=False)
                        bus.send(message)
                        response = {"status": "success", "message": f"Message '{msg_name}' sent."}
                    except Exception as e:
                        response = {"status": "error", "message": f"Failed to send message: {e}"}

            # --- ZeroMQ REP: Send response back to frontend ---
            command_rep_socket.send_json(response)

        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN: # No message received within timeout (not typical for REP)
                pass
            else:
                print(f"ZeroMQ error in command handler: {e}")
                break
        except Exception as e:
            print(f"Error handling command: {e}")
            command_rep_socket.send_json({"status": "error", "message": f"Internal server error: {e}"})

def create_dummy_dbc():
    """Creates a dummy DBC file if it doesn't exist."""
    if not os.path.exists(DBC_FILE_PATH):
        print(f"'{DBC_FILE_PATH}' not found. Creating a dummy one for testing.")
        with open(DBC_FILE_PATH, 'w') as f:
            f.write("""
VERSION ""

NS_ :
    NS_DESC_
    CM_
    BA_DEF_
    BA_
    VAL_
    EV_
    SGTYPE_
    SIG_VALTYPE_
    CAT_
    FILTER
    BA_DEF_DEF_
    BS_
    BU_

BS_:

BU_: ECU1

BO_ 2650805490 ENGINE_DATA: 8 ECU1
    SG_ Engine_RPM : 39|32@0+ (0.00390625,0) [0|8000] "rpm" Vector__XXX

BO_ 2650821874 FUELRate: 8 ECU1
    SG_ Injector_Pulsewidth : 7|31@0+ (0.00390625,0) [0|100] "ms" Vector__XXX
    SG_ Fuel_Flow : 39|32@0+ (0.00390625,0) [0|1000] "Pounds/hour" Vector__XX
    
BO_ 2650838258 CL_DUTY: 8 ECU1
    SG_ Closed_Loop_Status : 7|31@0+ (0.00390625,0) [0|1] "" Vector__XXX
    SG_ Duty_Cycle : 39|32@0+ (0.00390625,0) [0|100] "" Vector__XXX

BO_ 2650854642 AFR_CCOMP: 8 ECU1
    SG_ AFR_Left : 7|31@0+ (0.00390625,0) [0|1] "AFR" Vector__XXX
    SG_ Close_Loop_Comp : 39|32@0+ (0.00390625,0) [0|100] "%" Vector__XXX

BO_ 2650969330 Pedal_FPressure: 8 ECU1
    SG_ Pedal : 7|31@0+ (0.00390625,0) [0|100] "%" Vector__XXX
    SG_ Fuel_Pressure : 39|32@0+ (0.00390625,0) [0|1000] "PSI" Vector__XXX

BO_ 2651084018 Gear_n20: 8 ECU1
    SG_ Gear : 7|31@0+ (0.00390625,0) [0|5] "Gear" Vector__XXX
    SG_ n20_not_today : 39|32@0+ (0.00390625,0) [0|1000] "n2o" Vector__XXX

BO_  2650871026 AFR: 8 ECU1
    SG_ Target_AFR : 7|31@0+ (0.00390625,0) [0|5] "AFR" Vector__XXX
    SG_ AFR_Right : 39|32@0+ (0.00390625,0) [0|1000] "lamda" Vector__XXX

BO_ 2650887410 Timing_avAFR: 8 ECU1
    SG_ Timing : 7|31@0+ (0.00390625,0) [0|100] "Degrees" Vector__XXX
    SG_ Average_AFR : 39|32@0+ (0.00390625,0) [0|1000] "Lamda" Vector__XXX

BO_ 2650903794 MAP_knock: 8 ECU1
    SG_ MAP : 7|31@0+ (0.00390625,0) [0|400] "mpa" Vector__XXX
    SG_ knock : 39|32@0+ (0.00390625,0) [0|1] "n2o" Vector__XXX

BO_ 2650920178 MAT_TPS: 8 ECU1
    SG_ MAT : 7|31@0+ (0.00390625,0) [0|400] "degrees" Vector__XXX
    SG_ TPS : 39|32@0+ (0.00390625,0) [0|1] "%" Vector__XXX

BO_ 2650936562 Baro_CTS: 8 ECU1
    SG_ Baro : 7|31@0+ (0.00390625,0) [0|400] "mpa" Vector__XXX
    SG_ CTS : 39|32@0+ (0.00390625,0) [0|1] "Degrees" Vector__XXX

BO_ 2650936562 OIL_Battery: 8 ECU1
    SG_ Oil : 7|31@0+ (0.00390625,0) [0|400] "PSI" Vector__XXX
    SG_ voltage : 39|32@0+ (0.00390625,0) [00.0|99.9] "voltage" Vector__XXX

BO_ 2651100402 lineP_speed: 8 ECU1
    SG_ lineP : 7|31@0+ (0.00390625,0) [0|400] "PSI" Vector__XXX
    SG_ speed : 39|32@0+ (0.00390625,0) [0|150] "MPH" Vector__XXX





""")
        print("Dummy 'test.dbc' created. You can modify it or replace it with a real one.")

if __name__ == "__main__":
    create_dummy_dbc()

    # --- ZeroMQ Setup: Bind sockets ---
    can_pub_socket.bind(f"tcp://*:{PUB_PORT}") # * means listen on all network interfaces
    command_rep_socket.bind(f"tcp://*:{REQ_REP_PORT}")
    print(f"Backend listening for commands on tcp://*:{REQ_REP_PORT}")
    print(f"Backend publishing CAN data on tcp://*:{PUB_PORT}")

    # Start the CAN message listener in a separate thread
    can_listener_thread = threading.Thread(target=can_message_listener_thread, daemon=True)
    can_listener_thread.start()

    # Handle commands in the main thread (blocking for REQ/REP)
    try:
        handle_commands()
    except KeyboardInterrupt:
        print("Backend shutting down.")
    finally:
        shutdown_can_bus() # Ensure bus is shut down on exit
        # --- ZeroMQ Teardown: Close sockets and context ---
        can_pub_socket.close()
        command_rep_socket.close()
        context.term()
        print("ZeroMQ sockets closed and context terminated.")
