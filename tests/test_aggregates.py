import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aggregates import avg, median, format_minute


def test_avg_simple():
    assert avg([10.0, 20.0, 30.0]) == 20.0

def test_median_odd():
    assert median([3.0, 1.0, 2.0]) == 2.0

def test_median_even():
    # среднее двух центральных
    assert median([1.0, 2.0, 3.0, 4.0]) == 2.5

def test_format_minute_truncates_seconds():
    # секунды отбрасываются, остаётся hh:mm (ожидание берём из того же
    # локального времени, чтобы тест не зависел от таймзоны машины)
    import datetime
    dt = datetime.datetime(2026, 6, 15, 13, 45, 30)
    ts_ms = int(dt.timestamp() * 1000)
    assert format_minute(ts_ms) == dt.strftime("%H:%M")
