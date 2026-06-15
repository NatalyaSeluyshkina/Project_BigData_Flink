import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generator import build_event


def test_build_event_has_all_fields():
    ev = build_event([1, 2, 3, 4])
    assert set(ev.keys()) == {
        "device_id", "type_id", "event_time", "temperature", "humidity"
    }

def test_build_event_type_id_from_allowed_set():
    ev = build_event([1, 2, 3, 4])
    assert ev["type_id"] in [1, 2, 3, 4]

def test_build_event_ranges_and_types():
    ev = build_event([1])
    assert ev["type_id"] == 1
    assert isinstance(ev["event_time"], int)
    assert 15.0 <= ev["temperature"] <= 35.0
    assert 20.0 <= ev["humidity"] <= 90.0
