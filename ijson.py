import json
from typing import Iterable, Any

# Minimal stub for the ``ijson`` streaming API used in tests.
# ``items`` simply loads the entire JSON file and yields each item from a list.
def items(file, prefix) -> Iterable[Any]:
    data = json.load(file)
    if prefix == "item" and isinstance(data, list):
        for element in data:
            yield element
    else:
        # Fallback behaviour: return empty generator
        return iter(())
