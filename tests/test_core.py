import logging
from ingestion_worker.utils.logging import get_logger

def test_get_logger_simple():
    """A very simple test to ensure get_logger returns a Logger instance. This is a default test for CI/CD.
    Most of the important tests are located in scripts/test_*.py. Those tests require environment variables to be set.
    But those should not be pushed to a public repository. Besides, some of those tests cost Gemini API Tokens. Therefore,
    those tests should be restricted to local development as opposed to push to Github.
    """
    logger = get_logger("simple_test")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "simple_test"
