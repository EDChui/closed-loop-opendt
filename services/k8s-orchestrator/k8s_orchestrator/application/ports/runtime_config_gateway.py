from typing import AsyncIterator, Protocol

from k8s_orchestrator.application.events import ConfigChange

class RuntimeConfigGateway(Protocol):
     def __aiter__(self) -> AsyncIterator[ConfigChange]: ...
