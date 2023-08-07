# This is OPC UA Client - Demonstration of User
import asyncio
import logging

from asyncua import Client, ua

url_aas_universal_robot_ur5e = "opc.tcp://192.168.157.240:4840"
namespace = "http://examples.freeopcua.github.io"

nodeID_isBusy = "n=3;i=1604"

async def main():
    async with Client(url=url_aas_universal_robot_ur5e) as client:
        while True:
            node = client.get_node(nodeID_isBusy)
            value = await node.read_value()
            print("Robot busy = " + str(value))

if __name__ == "__main__":
    asyncio.run(main())