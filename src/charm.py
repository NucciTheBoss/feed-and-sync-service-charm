#!/usr/bin/env python3
# Copyright 2022 Canonical
# See LICENSE file for licensing details.

"""My ping pong charm!"""

import logging
import time
from typing import Any

from hpctlib.ops.charm.service import ServiceCharm
from hpctlib.interface import codec, checker
from hpctlib.interface import interface_registry
from hpctlib.interface.base import Value
from hpctlib.interface.relation import RelationSuperInterface, UnitBucketInterface
from ops.charm import ActionEvent, ConfigChangedEvent, RelationChangedEvent
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus


logger = logging.getLogger(__name__)


class FeedAndSyncSuperInterface(RelationSuperInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.interface_classes[("provider", "unit")] = self.FeedAndSyncInterface
        self.interface_classes[("requirer", "unit")] = self.FeedAndSyncInterface

    class FeedAndSyncInterface(UnitBucketInterface):
        message = Value(codec.String(), "")
        origin = Value(codec.String(), "")
        next_holder = Value(codec.String(), "")
        cycles_complete = Value(codec.Integer(), 0, checker.IntegerRange(0, None))
        times_passed = Value(codec.Integer(), 0, checker.IntegerRange(0, None))
        times_received = Value(codec.Integer(), 0, checker.IntegerRange(0, None))
        time_elapsed = Value(codec.Float(), 0.0, checker.FloatRange(0.0, None))
        timestamp = Value(codec.Float(), 0.0, checker.FloatRange(0.0, None))


class FeedAndSyncCharm(ServiceCharm):

    _stored = StoredState()
    _MAX_CYCLES_KEY = "max_cycles"
    _DELAY_KEY = "delay"

    def __init__(self, *args):
        super().__init__(*args)

        self.i_send = interface_registry.load(
            "relation-feed-and-sync", self, "ping-send"
        )
        self.i_receive = interface_registry.load(
            "relation-feed-and-sync", self, "ping-receive"
        )

        self.framework.observe(self.on.ping_action, self._on_ping_action)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(
            self.on.ping_receive_relation_changed,
            self._on_ping_receive_relation_changed,
        )
        self._stored.set_default(
            bucket={self._MAX_CYCLES_KEY: None, self._DELAY_KEY: 0}
        )

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        """When the configuration of the service is updated."""
        storage = self._stored.bucket
        if (max_cycles := self.config.get("max-cycles")) != storage[
            self._MAX_CYCLES_KEY
        ]:
            logger.info(f"Updating max cycles to {max_cycles}.")
            storage.update({self._MAX_CYCLES_KEY: max_cycles})
        if (delay := self.config.get("delay")) != storage[self._DELAY_KEY]:
            logger.info(f"Updating total delay to {delay}.")
            storage.update({self._DELAY_KEY: delay})

    def _on_ping_receive_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handler for message is sent over ping-send relation."""
        logger.info("RelationChanged event detected...")
        if (
            "message" not in event.relation.data[event.unit]
            or event.relation.data[event.unit].get("message") == ""
        ):
            logger.info("Key 'message' is not present or is empty.")
            return
        else:
            recv_bucket = self.i_receive.select(event.unit)
            send_bucket = self.i_send.select(self.unit)

            logger.info("Checking cycles complete...")
            c = recv_bucket.cycles_complete + 1
            if recv_bucket.origin == self.unit.name:
                if self._stored.bucket[self._MAX_CYCLES_KEY] == c:
                    self.unit.status = ActiveStatus(
                        (
                            "Max cycles reached. "
                            f"Time to completion is {recv_bucket.time_elapsed} seconds."
                        )
                    )
                    return
                else:
                    send_bucket.cycles_complete = c
            else:
                send_bucket.cycles_complete = recv_bucket.cycles_complete

            logger.info("Updating token and sending to next holder...")
            self.unit.status = ActiveStatus(
                (
                    f"M: {recv_bucket.message} "
                    f"H: {recv_bucket.next_holder} "
                    f"P: {recv_bucket.times_passed} "
                    f"R: {recv_bucket.times_received + 1} "
                    f"C: {recv_bucket.cycles_complete} "
                    f"T: {recv_bucket.time_elapsed:.2f}"
                )
            )
            if (delay := self._stored.bucket[self._DELAY_KEY]) > 0:
                time.sleep(delay)
            r = self.model.get_relation("ping-send")
            target = [u.name for u in r.units if u.app is not self.app]
            send_bucket.message = recv_bucket.message
            send_bucket.origin = recv_bucket.origin
            send_bucket.next_holder = target[0]
            if send_bucket.cycles_complete != recv_bucket.cycles_complete:
                pass
            else:
                send_bucket.cycles_complete = recv_bucket.cycles_complete
            send_bucket.times_passed = recv_bucket.times_passed + 1
            send_bucket.times_received = recv_bucket.times_received + 1
            timestamp = time.time()
            send_bucket.time_elapsed = recv_bucket.time_elapsed + (
                timestamp - recv_bucket.timestamp
            )
            send_bucket.timestamp = timestamp
            self.unit.status = ActiveStatus()

    def _on_ping_action(self, event: ActionEvent) -> None:
        """Handler for when the ping action is invoked."""
        logger.info("Ping action event recieved. Starting...")
        r = self.model.get_relation("ping-send")
        target = [u.name for u in r.units if u.app is not self.app]

        logger.info("Constructing token...")
        send_bucket = self.i_send.select(self.unit)
        send_bucket.message = event.params["token"]
        send_bucket.origin = self.unit.name
        send_bucket.next_holder = target[0]
        send_bucket.cycles_complete = 0
        send_bucket.times_passed = 1
        send_bucket.times_received = 0
        send_bucket.time_elapsed = 0.0
        send_bucket.timestamp = time.time()

        logger.info(f"Sending token to {send_bucket.next_holder}...")
        if (delay := self._stored.bucket[self._DELAY_KEY]) > 0:
            time.sleep(delay)


if __name__ == "__main__":
    interface_registry.register("relation-feed-and-sync", FeedAndSyncSuperInterface)

    main(FeedAndSyncCharm)
