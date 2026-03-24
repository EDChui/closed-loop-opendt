"""Base producer class for K8S-Trace-Bridge threaded producers."""

import logging
import threading
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """Base class for threaded workers.

    Provides common functionality for all producers:
    - Thread lifecycle management
    """

    def __init__(
        self,
        name: str | None = None,
        start_barrier: threading.Barrier | None = None,
    ):
        """Initialize the base worker.

        Args:
            name: Optional worker name for logging
            start_barrier: Optional barrier for synchronized startup across workers
        """
        self.name = name or self.__class__.__name__
        self._start_barrier = start_barrier

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        logger.info(f"Initialized {self.name}")

    @abstractmethod
    def run(self) -> None:
        """Run the worker (main logic).

        This method should be implemented by subclasses and will be
        executed in a separate thread.
        """
        pass

    def start(self) -> None:
        """Start the worker in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning(f"{self.name} is already running")
            return

        logger.info(f"Starting {self.name} in background thread...")
        self._thread = threading.Thread(target=self._run_wrapper, daemon=True, name=self.name)
        self._thread.start()
        logger.info(f"{self.name} started")

    def _run_wrapper(self) -> None:
        """Wrapper around run() for exception handling and cleanup."""
        try:
            # Wait at barrier if one is configured (synchronize startup)
            if self._start_barrier:
                logger.info(f"{self.name} waiting at start barrier...")
                self._start_barrier.wait()
                logger.info(f"{self.name} released from barrier, starting...")

            self.run()
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(f"Error in {self.name}: {e}", exc_info=True)
        finally:
            self._cleanup()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the producer and wait for thread to finish.

        Args:
            timeout: Maximum time to wait for thread termination in seconds
        """
        if self._thread is None or not self._thread.is_alive():
            logger.debug(f"{self.name} is not running")
            return

        logger.info(f"Stopping {self.name}...")
        self._stop_event.set()

        # Wait for thread to finish
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            logger.warning(f"{self.name} did not terminate within {timeout}s")
        else:
            logger.info(f"{self.name} stopped")

    def _cleanup(self) -> None:
        """Optional cleanup hook for subclasses."""
        pass

    def is_running(self) -> bool:
        """Check if the worker thread is running.

        Returns:
            True if thread is alive, False otherwise
        """
        return self._thread is not None and self._thread.is_alive()

    def should_stop(self) -> bool:
        """Check if stop has been requested.

        Returns:
            True if stop event is set, False otherwise
        """
        return self._stop_event.is_set()

    def wait_interruptible(self, seconds: float) -> bool:
        """Wait for specified seconds, but can be interrupted by stop event.

        Args:
            seconds: Time to wait in seconds

        Returns:
            True if interrupted (should stop), False if timed out normally
        """
        return self._stop_event.wait(timeout=seconds)
