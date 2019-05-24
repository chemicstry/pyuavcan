#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#

import candle_driver
import time
import copy
import threading

from .common import DriverError, TxQueueFullError, CANFrame, AbstractDriver
from .timestamp_estimator import TimestampEstimator, SourceTimeResolver

try:
    import queue
except ImportError:
    # noinspection PyPep8Naming,PyUnresolvedReferences
    import Queue as queue

try:
    import candle_driver
except ImportError:
    candle_driver = None

TIMESTAMP_OVERFLOW_PERIOD = 4294.967295 # 32 bit counter running at 1 MHz
DEFAULT_FIXED_RX_DELAY = 0.0002

DEFAULT_BITRATE = 1000000

TX_QUEUE_SIZE = 100
IPC_COMMAND_STOP = 'stop'

class CandleCAN(AbstractDriver):
    def __init__(self, device_name, bitrate=None, **kwargs):
        super(CandleCAN, self).__init__()

        if candle_driver is None:
            raise ModuleNotFoundError('To use this driver please install candle_driver package.')

        # split name into device name and channel number
        name_split = device_name.split('#')

        # find device with provided name
        self.device = self._find_device(name_split[0])

        if self.device is None:
            raise DriverError('Candle device `%s` not found' % device_name)

        if not self.device.open():
            raise DriverError('Candle device `%s` open failed. Device in use?' % device_name)

        # extract channel number from name like 'candle_aabbcc#2'
        try:
            channel_num = int(name_split[1])
        except:
            channel_num = 0

        # try acquiring specified channel
        # unfortunatelly, other channels can not be used by other applications
        # because device can be opened by a single process only (single process can use multiple channels)
        self.channel = self.device.channel(channel_num)

        if self.channel is None:
            raise DriverError('Candle device `%s` channel %d open failed' % (device_name, channel_num))

        bitrate = bitrate if bitrate is not None else DEFAULT_BITRATE
        if not self.channel.set_bitrate(bitrate):
            raise DriverError('Failed to set candle bitrate to %d' % bitrate)

        if not self.channel.start():
            raise DriverError('Candle device `%s` channel %d start failed' % (device_name, channel_num))

        # setup time estimators as device has non absolute overflowing clock
        self.ts_estimator_mono = TimestampEstimator(source_clock_overflow_period=TIMESTAMP_OVERFLOW_PERIOD,
                                                    fixed_delay=DEFAULT_FIXED_RX_DELAY)
        self.ts_estimator_real = copy.deepcopy(self.ts_estimator_mono)

        # tx queue is used because driver will block once USB buffer is full
        self.tx_queue = queue.Queue(TX_QUEUE_SIZE)

        self.stopping = False
        self.tx_worker_thread = threading.Thread(target=self._tx_worker, daemon=True)
        self.tx_worker_thread.start()

    """
    Finds first device that matches the provided name. For exmaple:
    candle_aabbcc - returns candle device with aabbcc serial number (if present)
    candle - returns first candle device
    """
    def _find_device(self, name):
        devices = candle_driver.list_devices()
        for device in devices:
            device_name = device.name()
            if name in device_name:
                return device
        return None

    def _tx_worker(self):
        while not self.stopping:
            try:
                message_id, message = self.tx_queue.get(True, 0.1)

                # TX process can be threaded because channel.write allows
                # python threads while it blocks (waiting for USB transmission)
                if not self.channel.write(message_id, bytes(message)):
                    raise DriverError('CAN write failed')
            except queue.Empty:
                pass

    def close(self):
        self.stopping = True
        self.tx_worker_thread.join()
        self.channel.stop()
        self.device.close()

    def receive(self, timeout=0):
        try:
            # convert timeout to ms
            frame_type, can_id, can_data, extended, ts_hardware = self.channel.read(int(timeout*1000))
            local_ts_mono = time.monotonic() # capture local time immediately
            local_ts_real = time.time()

            # hardware ts is in usec
            ts_mono = self.ts_estimator_mono.update(ts_hardware/1e6, local_ts_mono)
            ts_real = self.ts_estimator_real.update(ts_hardware/1e6, local_ts_real)

            frame = CANFrame(can_id, can_data, bool(extended), ts_mono, ts_real)

            if frame_type == candle_driver.CANDLE_FRAMETYPE_RECEIVE:
                self._rx_hook(frame)
                return frame
            elif frame_type == candle_driver.CANDLE_FRAMETYPE_ECHO:
                self._tx_hook(frame)
            else:
                raise DriverError('Unexpected frame type received: %r' % frame)
        except TimeoutError:
            pass

    def send(self, message_id, message, extended=False):
        # append extended id flag to can id if frame is extended
        message_id = message_id if extended is False else message_id | candle_driver.CANDLE_ID_EXTENDED
        try:
            self.tx_queue.put_nowait((message_id, message))
        except queue.Full:
            raise TxQueueFullError()

