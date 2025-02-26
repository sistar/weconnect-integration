from unittest.mock import Mock, patch

import pytest
from weconnect import addressable

from we_connect_event_listener import on_we_connect_event


@pytest.fixture
def mock_collection():
    with patch("we_connect_event_listener.collection") as mock:
        yield mock


def test_parking_position_events(mock_collection):
    # Mock AddressableAttribute objects
    # pylint: disable=E1101
    lat_attr = Mock(spec=addressable.AddressableAttribute)
    lat_attr.getGlobalAddress.return_value = (
        "/vehicles/WVGZZZE25SE030804/parking/parkingPosition/latitude"
    )
    lat_attr.value = 52.902548
    lat_attr.lastChange = "2025-02-06 17:09:14+00:00"

    lon_attr = Mock(spec=addressable.AddressableAttribute)
    lon_attr.getGlobalAddress.return_value = (
        "/vehicles/WVGZZZE25SE030804/parking/parkingPosition/longitude"
    )
    lon_attr.value = 9.763807
    lon_attr.lastChange = "2025-02-06 17:09:14+00:00"

    timestamp_attr = Mock(spec=addressable.AddressableAttribute)
    timestamp_attr.getGlobalAddress.return_value = (
        "/vehicles/WVGZZZE25SE030804/parking/parkingPosition/carCapturedTimestamp"
    )
    timestamp_attr.value = "2025-02-06 17:09:14+00:00"
    timestamp_attr.lastChange = "2025-02-06 17:09:14+00:00"

    # Mock document in MongoDB
    mock_doc = {"_id": "test_id", "status": "new"}
    mock_collection.find_one.return_value = mock_doc

    # Test latitude events
    on_we_connect_event(lat_attr, addressable.AddressableLeaf.ObserverEvent.ENABLED)
    on_we_connect_event(
        lat_attr, addressable.AddressableLeaf.ObserverEvent.VALUE_CHANGED
    )

    mock_collection.insert_one.assert_called_with(
        {
            "latitude": 52.902548,
            "status": "latitude received",
            "timestamp": lat_attr.lastChange,
        },
    )

    # Test longitude events
    on_we_connect_event(lon_attr, addressable.AddressableLeaf.ObserverEvent.ENABLED)
    on_we_connect_event(
        lon_attr, addressable.AddressableLeaf.ObserverEvent.VALUE_CHANGED
    )

    mock_collection.update_one.assert_called_with(
        {"_id": "test_id"},
        {"$set": {"longitude": 9.763807, "status": "longitude received"}},
    )

    # Test timestamp events
    on_we_connect_event(
        timestamp_attr, addressable.AddressableLeaf.ObserverEvent.ENABLED
    )
    on_we_connect_event(
        timestamp_attr, addressable.AddressableLeaf.ObserverEvent.VALUE_CHANGED
    )

    mock_collection.update_one.assert_called_with(
        {"_id": "test_id"},
        {
            "$set": {
                "carCapturedTimestamp": "2025-02-06 17:09:14+00:00",
                "status": "carCapturedTimestamp received",
            }
        },
    )


def test_disabled_event(mock_collection):
    # pylint: disable=E1101
    attr = Mock(spec=addressable.AddressableAttribute)
    attr.getGlobalAddress.return_value = (
        "/vehicles/WVGZZZE25SE030804/parking/parkingPosition/latitude"
    )

    on_we_connect_event(attr, addressable.AddressableLeaf.ObserverEvent.DISABLED)
    # Assert print statement was called (requires additional mocking if needed)
