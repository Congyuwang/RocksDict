from abc import abstractmethod, ABC
from typing import Any, List, Tuple


class ADB(ABC):
    """All tests use ADB class as db instance class.

    All actual DB must inherit this class.
    """
    def insert_raw(self, key: bytes, value: bytes) -> None:
        raise NotImplementedError("method unimplemented")

    def insert(self, key: Any, value: Any) -> None:
        raise NotImplementedError("method unimplemented")

    def get_raw(self, key: Any) -> Any:
        raise NotImplementedError("method unimplemented")

    def get(self, key: Any) -> Any:
        raise NotImplementedError("method unimplemented")

    def batch_insert_raw(self, kv_list: List[Tuple[bytes, bytes]]) -> None:
        raise NotImplementedError("method unimplemented")

    def batch_insert(self, kv_list: List[Tuple[Any, Any]]) -> None:
        raise NotImplementedError("method unimplemented")

    def multi_get_raw(self, key: List) -> List:
        raise NotImplementedError("method unimplemented")

    def multi_get(self, key: List) -> List:
        raise NotImplementedError("method unimplemented")

    def delete(self, key: Any) -> None:
        raise NotImplementedError("method unimplemented")

    def delete_raw(self, key: bytes) -> None:
        raise NotImplementedError("method unimplemented")

    def delete_range(self, start: Any, end: Any) -> None:
        raise NotImplementedError("method unimplemented")

    def delete_range_raw(self, start: bytes, end: bytes) -> None:
        raise NotImplementedError("method unimplemented")

    def contains(self, key: Any) -> bool:
        raise NotImplementedError("method unimplemented")

    def contains_raw(self, key: bytes) -> bool:
        raise NotImplementedError("method unimplemented")

    @abstractmethod
    def destroy(self) -> None: ...
