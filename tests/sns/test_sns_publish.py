import pytest
from unittest.mock import MagicMock, patch
from automation.sns import SNS 

@pytest.fixture
def sns():
    sns = SNS(subject="Test",
              message="{ }",
              subject_writer=None,
              message_writer=None)

    sns.client = MagicMock()
    return sns

def test_publish_not_called_when_message_empty(sns):
    input = None # adjust as needed for your test scenario
    
    sns.publish(input)

    sns.client.publish.assert_not_called()
