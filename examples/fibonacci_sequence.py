import asyncio
import logging
import multiprocessing
import sys

from mycometo import Device, CoreEvents, EngineEvents, IPCEngine, IPCRoutedConnection, IPCPacket, Role, SingleDeviceRole



logger = logging.getLogger(__name__)



COLLECTOR_ROLE_NAME = "role_collector"
WORKER_ROLE_NAME = "role_worker"


class CustomSignals:
    START_CALC = "fib_start_calc"


async def fib(n):
    # Mostly stolen from https://github.com/drujensen/fib/blob/master/fib.py
    if n <= 1: return n
    await asyncio.sleep(0)  # Since this will kill the CPU core/thread, we need to let the websocket breath.
    return await fib(n - 1) + await fib(n - 2)


class Collector(Device):
    def __init__(self, fibonacci_position: int, startup_timeout: float = 1.0, fibonacci_timeout: float = 800.0):
        super().__init__()
        self._fibonacci_position = fibonacci_position
        self.connected_workers: dict[str, IPCRoutedConnection] = {}
        self._startup_timeout = startup_timeout
        self._fibonacci_timeout = fibonacci_timeout

    def on_set_engine(self, engine: IPCEngine):
        # Since the Device isn't initialized with an Engine, we need to interact with it when it's already set.
        self.engine.events.add_listener(self.on_ready, EngineEvents.ENGINE_READY)

    async def on_ready(self):
        logger.info(
            "Waiting %s seconds before telling nodes to find fibonacci position #%s...",
            self._startup_timeout, self._fibonacci_position
        )
        keep_waiting = True
        while keep_waiting:
            try:
                await self.events.wait_for(CoreEvents.ROUTED_CONN_CONNECTION, timeout=self._startup_timeout)
            except asyncio.TimeoutError:
                keep_waiting = False
                logger.info("Timeout went through, continuing.")
            else:
                # Joined node will be mentioned in on_chat_connection.
                logger.debug("Timeout reset, continuing to wait...")

        # Clear the disconnected nodes before start...
        logger.debug("Clearing out any possibly disconnected nodes...")
        for conn in self.connected_workers.copy().values():
            if not conn.is_open:
                self.connected_workers.pop(conn.dest_name)

        requests = []
        logger.debug("Building up requests...")
        for conn in self.connected_workers.values():
            requests.append(conn.send_request(
                self._fibonacci_position,
                CustomSignals.START_CALC,
                timeout=self._fibonacci_timeout
            ))

        logger.debug("Gathering and sending requests.")
        responses: list[IPCPacket | BaseException] = await asyncio.gather(*requests, return_exceptions=True)

        logger.debug("Either all workers finished or timed out, displaying results...")
        connected_uuids = set(self.connected_workers.keys())

        if responses:
            for packet in responses:
                if isinstance(packet, IPCPacket):
                    connected_uuids.discard(packet.origin_name)
                    logger.info("Worker %s responded back with the result of: %s", packet.origin_name, packet.data)
                else:
                    logger.error(
                        "Received error %s %s when sending/waiting for request.",
                        type(packet), packet,
                    )
        else:
            logger.warning("No workers responded back.")

        if connected_uuids:
            for missing_uuid in connected_uuids:
                logger.warning("Worker %s either errored or didn't respond in time...", missing_uuid)
        else:
            logger.info("No workers failed to respond.")

        logger.info("Press Control + C to quit.")

    async def on_chat_connection(self, chat: IPCRoutedConnection):
        # When an incoming chat connection is successfully received...
        logger.info("Connection from worker %s received, storing connection.", chat.dest_name)
        self.connected_workers[chat.dest_name] = chat




class Worker(Device):
    def __init__(self):
        super().__init__()
        self.collector: IPCRoutedConnection | None = None

        self.events.add_listener(self.on_start_calc, CustomSignals.START_CALC)

    def on_set_engine(self, engine: IPCEngine):
        self.engine.events.add_listener(self.on_role_added, EngineEvents.ROLE_ADDED)

    async def on_role_added(self, role_name: str, node_uuid: str):
        if role_name == COLLECTOR_ROLE_NAME:
            logger.debug("Found collector role %s, attempting to connect to it.", role_name)
            # The role will route us to the Collector device.
            # Routed connections are typically used for request-response.
            self.collector = self.engine.map.routed_conn_to(self, role_name)
            await self.collector.open()
            # While opening a connection returns instantly, it may take time for fully routed communication to
            #  be established.
            await self.collector.wait_until_comm_ready()
            logger.info("Connection to Collector successful!")


    async def on_start_calc(self, packet: IPCPacket):
        logger.debug("Received orders from Collector to start calculating position %s", packet.data)

        # In the event that we're told to shut down while working on the sequence, we need to be able to stop it.
        task = await self.engine.run_stoppable_task(fib(packet.data))
        if not task.done():
            logger.warning("Calculation cancelled, not going to send a response to Collector.")
        else:
            if packet.is_request:
                logger.debug("Finished calculation, sending to Collector.")
                await packet.send_response(task.result())
            else:
                logger.info(
                    "Finished calculation, but the Collector didn't send a request. Here's the result: %s",
                    task.result()
                )


def create_host(fibonacci_position: int, server_port):
    engine = IPCEngine()

    # Only one collector should exist in the IPC setup, so we use a SingleDeviceRole.
    collector_role = SingleDeviceRole(COLLECTOR_ROLE_NAME)
    collector = Collector(fibonacci_position)
    collector_role.add_device(collector)
    engine.add_role(collector_role)

    # Because nothing connects to the workers, a generic role will work fine.
    worker_role = Role(WORKER_ROLE_NAME)
    worker_role.add_device(Worker())
    engine.add_role(worker_role)

    engine.run_server(port=server_port)



def create_worker(host: str, server_port: int):
    engine = IPCEngine()
    wok_role = Role(WORKER_ROLE_NAME)
    wok_role.add_device(Worker())
    engine.add_role(wok_role)
    engine.run_serverless(discover_nodes=[(host, server_port)])



if __name__ == "__main__":
    log_console_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(log_console_handler)
    logger.setLevel(logging.INFO)

    import time

    fib_position = 25
    # For maximum CPU usage, set this to 1 below your core/thread count (the host process also has a worker.
    worker_count = 15
    port = 6666

    host_process = multiprocessing.Process(target=create_host, args=[fib_position, port])
    host_process.start()

    worker_processes: list[multiprocessing.Process] = []
    for i in range(worker_count):
        p = multiprocessing.Process(target=create_worker, args=["0.0.0.0", port])
        p.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        time.sleep(2)
