# This is OPC UA Server - Based on AAS
import asyncio
import logging
import queue

from asyncua import Server, ua
from asyncua.common.methods import uamethod

# Import of Client functions. This Server is connected to Server of robot control only via the Client
from OPCUA_Client_Communication_with_OPCUA_Server_UR5e import read_var, read_pos, write_start, write_service, write_pos

# Declaring nodeID of UR5e robot control`s OPC UA server for communication
nodeID_start = "ns=2;s=start"
nodeID_isBusy = "ns=2;s=isBusy"
nodeID_service_id = "ns=2;s=service"
nodeID_isUnderService = "ns=2;s=isUnderService"
nodeID_pick_id = "ns=2;s=pick_id"
nodeID_pick_dir = "ns=2;s=pick_dir"
nodeID_place_id = "ns=2;s=place_id"
nodeID_place_dir = "ns=2;s=place_dir"

# Fifo queue for storing pick and place requests, pap_quqeue_length defines the max amount of requests stored
pap_queue_length = 3
pap_queue = queue.Queue(pap_queue_length)

# Functions basically just write variables that are used in the robot program to start certain processes
# and/or read variables to monitor its state. Connection realized using Client functions.

# Declare fucntion of service operation
# Put robot into one of its service positions/programs, return True when program runs through
@uamethod
async def service(nodeID, service_id):
    await asyncio.create_task(write_service(nodeID_service_id, service_id))
    return True

# Declare function of checking number of queue 
# Queueing pick and place requests, return False when queue already full, True when it's not
@uamethod
async def pick_and_place(nodeID, pick_id, pick_dir, place_id, place_dir):
    if pap_queue.full():
        return False
    else:
        pap_queue.put([pick_id, pick_dir, place_id, place_dir])
        return True

# Declare function of pick and place operation
# Start certain robot process
async def pap_action(pick_id, pick_dir, place_id, place_dir):

    # Set id of module and its direction, start Process
    # Pick
    await asyncio.create_task(write_pos(nodeID_pick_id, nodeID_pick_dir, pick_id, pick_dir))
    await asyncio.create_task(read_pos(nodeID_pick_id, nodeID_pick_dir))
    # Place
    await asyncio.create_task(write_pos(nodeID_place_id, nodeID_place_dir, place_id, place_dir))
    await asyncio.create_task(read_pos(nodeID_place_id, nodeID_place_dir))
    # Start
    await asyncio.create_task(write_start(nodeID_start, True))


async def main():
    logger = logging.getLogger(__name__)

    # Setup server
    server = Server()
    await server.init()
    # Please set your owned end point
    server.set_endpoint("opc.tcp://0.0.0.0:4840")
    server.set_server_name("Digital Factory Transfer")

    # Import AAS of universal robot ur5e xml 
    aas_nodes = await server.import_xml("AAS_Universal_Robot_UR5e.xml")
    nodes = [server.get_node(node) for node in aas_nodes]

    # Get node from AAS of universal robot ur5e xml and link method for function of checking number of queue 
    pick_and_place_method_from_xml = server.get_node("ns=3;i=1571")
    server.link_method(pick_and_place_method_from_xml, pick_and_place)

    # Get node from AAS of universal robot ur5e xml and link method for function of service
    service_method_from_xml = server.get_node("ns=3;i=1595")
    server.link_method(service_method_from_xml, service)

    # Get node from AAS of universal robot ur5e xml for occupancy status
    is_busy_node_from_xml = server.get_node("ns=3;i=1604")

    # Get node from AAS of universal robot ur5e xml for maintenance status
    is_under_service_node_from_xml = server.get_node("ns=3;i=1611")
    is_at_service_position_node_from_xml = server.get_node("ns=3;i=1618")

    # Get node from AAS of universal robot ur5e xml for queue management
    number_of_queue_node_from_xml = server.get_node("ns=3;i=1625")

    # Get node from AAS of universal robot ur5e xml for information of pick and place operation
    current_picking_direction_node_from_xml = server.get_node("ns=3;i=1632")
    current_placing_direction_node_from_xml = server.get_node("ns=3;i=1639")
    current_picking_module_id_node_from_xml = server.get_node("ns=3;i=1646")
    current_placing_module_id_node_from_xml = server.get_node("ns=3;i=1653")

    # Setup namespace
    uri = "https://github.com/heMeyer/UR5_OPCUA_1.git"
    idx = await server.register_namespace(uri)

    # Create root node for upcoming functions, variables
    objects = server.nodes.objects

    # Prepare arguments for methods
    # explanation for structure will be depicted only one time for first argument

    # Pick and place operation

    pick_id = ua.Argument()  # Implementation as a argument
    pick_id.Name = "pick_id"  # Display name
    pick_id.DataType = ua.NodeId(ua.ObjectIds.Int32)  # Data type
    pick_id.ValueRank = -1  # Amount of array dimensions (-1 equals scalar value)
    pick_id.ArrayDimensions = []  # amount of values in each array dimension
    pick_id.Description = ua.LocalizedText("ID of the module for picking")  # Display explanation
    
    pick_dir = ua.Argument()
    pick_dir.Name = "pick_dir"
    pick_dir.DataType = ua.NodeId(ua.ObjectIds.Int32)
    pick_dir.ValueRank = -1
    pick_dir.ArrayDimensions = []
    pick_dir.Description = ua.LocalizedText("Direction of the direction for picking")

    place_id = ua.Argument()
    place_id.Name = "place_id"
    place_id.DataType = ua.NodeId(ua.ObjectIds.Int32)
    place_id.ValueRank = -1
    place_id.ArrayDimensions = []
    place_id.Description = ua.LocalizedText("ID of the module for placing")

    place_dir = ua.Argument()
    place_dir.Name = "place_dir"
    place_dir.DataType = ua.NodeId(ua.ObjectIds.Int32)
    place_dir.ValueRank = -1
    place_dir.ArrayDimensions = []
    place_dir.Description = ua.LocalizedText("Direction of the direction for placing")

    result_pap = ua.Argument()
    result_pap.Name = "result_pap"
    result_pap.DataType = ua.NodeId(ua.ObjectIds.Boolean)
    result_pap.ValueRank = -1
    result_pap.ArrayDimensions = []
    result_pap.Description = ua.LocalizedText("Call successfull")

    # service operation

    service_id = ua.Argument()
    service_id.Name = "service_id"
    service_id.DataType = ua.DataType = ua.NodeId(ua.ObjectIds.Int32)
    service_id.ValueRank = -1
    service_id.ArrayDimensions = []
    service_id.Description = ua.LocalizedText("Service Positions: 0 = none, 1-6 = Maintanance Positions")

    result_s = ua.Argument()
    result_s.Name = "result_s"
    result_s.DataType = ua.NodeId(ua.ObjectIds.Boolean)
    result_s.ValueRank = -1
    result_s.ArrayDimensions = []
    result_s.Description = ua.LocalizedText("Call successfull")

    # Populating address space
    await objects.add_method(idx, "pick_and_place", pick_and_place, [pick_id, pick_dir, place_id, place_dir],[result_pap])
    await objects.add_method(idx, "service", service, [service_id], [result_s])


    # Running Server
    logger.info("Starting Server!")
    async with server:
        while True:

            # Read/update variables from UR5e OPC UA server
            # For occupancy status
            robot_busy = await read_var(nodeID_isBusy)
            # For Maintenance Status
            is_under_service = await read_var(nodeID_isUnderService)
            service_position = await read_var(nodeID_service_id)
            # For Information of pick and place operation
            current_pick_direction = await read_var(nodeID_pick_dir)
            current_place_direction = await read_var(nodeID_place_dir)
            current_pick_module_id = await read_var(nodeID_pick_id)
            current_place_module_id = await read_var(nodeID_place_id)

            # Send pick and place instruction if one in queue
            if pap_queue.qsize() > 0 and not robot_busy:
                instruction = pap_queue.get()
                await asyncio.create_task(pap_action(instruction[0], instruction[1], instruction[2], instruction[3]))

            # Basic print server functions/helper functions
            print("Robot busy = " + str(robot_busy))
            print("Robot is under service = " + str(is_under_service))
            print("Robot is at service position = " + str(service_position))
            print("Queue size = " + str(pap_queue.qsize()))
            
            #write value back to AAS base OPC UA server
            # For occupancy status
            await is_busy_node_from_xml.write_value(robot_busy)
            await number_of_queue_node_from_xml.write_value(pap_queue.qsize())
            # For Maintenance Status
            await is_at_service_position_node_from_xml.write_value(service_position)
            await is_under_service_node_from_xml.write_value(is_under_service)
            # For Information of pick and place operation
            await current_picking_direction_node_from_xml.write_value(current_pick_direction)
            await current_placing_direction_node_from_xml.write_value(current_place_direction)
            await current_picking_module_id_node_from_xml.write_value(current_pick_module_id)
            await current_placing_module_id_node_from_xml.write_value(current_place_module_id)

            await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())
    
