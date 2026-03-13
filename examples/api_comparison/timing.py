"""
Timing utilities for benchmarking
"""

import time

# Global timing state for benchmarking
_timing_state: dict = {
    "neo_timing": None,
    "mongo_timing": None,
    "current_phase": None,
    "last_neo_timing": 0.0,
    "last_mongo_timing": 0.0,
}


def start_neo_timing():
    """Start timing NeoSQLite operations"""
    _timing_state["neo_timing"] = time.perf_counter()
    _timing_state["current_phase"] = "neo"


def end_neo_timing():
    """End timing NeoSQLite operations and return timing in ms"""
    if _timing_state["neo_timing"] is None:
        return 0.0
    timing = (time.perf_counter() - _timing_state["neo_timing"]) * 1000
    _timing_state["neo_timing"] = None
    _timing_state["current_phase"] = None
    _timing_state["last_neo_timing"] = timing
    return timing


def start_mongo_timing():
    """Start timing MongoDB operations"""
    _timing_state["mongo_timing"] = time.perf_counter()
    _timing_state["current_phase"] = "mongo"


def end_mongo_timing():
    """End timing MongoDB operations and return timing in ms"""
    if _timing_state["mongo_timing"] is None:
        return 0.0
    timing = (time.perf_counter() - _timing_state["mongo_timing"]) * 1000
    _timing_state["mongo_timing"] = None
    _timing_state["current_phase"] = None
    _timing_state["last_mongo_timing"] = timing
    return timing


def get_last_neo_timing() -> float:
    """Get the last NeoSQLite timing in ms"""
    return _timing_state["last_neo_timing"]


def get_last_mongo_timing() -> float:
    """Get the last MongoDB timing in ms"""
    return _timing_state["last_mongo_timing"]
