# -*- coding: utf-8 -*-
"""Чистые функции агрегации, используемые в оконной обработке Flink.
Вынесены отдельно, чтобы их можно было протестировать без запуска кластера."""
import statistics
from datetime import datetime


def avg(values):
    """Среднее арифметическое непустого списка чисел."""
    return sum(values) / len(values)


def median(values):
    """Точная медиана списка чисел (для чётной длины — среднее двух центральных)."""
    return statistics.median(values)


def format_minute(ts_ms):
    """Эпоха в миллисекундах -> строка локального времени вида 'hh:mm'."""
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%H:%M")
