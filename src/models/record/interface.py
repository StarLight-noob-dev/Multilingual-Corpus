"""Compatibility shim: expose IRecord under the module path
`src.models.record.interface` while keeping the implementation in
`record_interface.py`.
"""
from .record_interface import IRecord

__all__ = ["IRecord"]

