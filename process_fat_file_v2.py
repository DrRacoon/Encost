#!/bin/python3.12
"""
Подсчитать хэш.
"""

from hashlib import sha256
from typing import Final


MB: Final[int] = 1 << 20


hash_counter = sha256()
with open("path/to/book.txt", "br") as file:
    for chunk in iter(lambda: file.read(MB), b""):
        hash_counter.update(chunk)
print(hash_counter.hexdigest())        
