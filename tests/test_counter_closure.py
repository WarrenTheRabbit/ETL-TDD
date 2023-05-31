import pytest
from testbook import testbook


cells_to_test = ['counter']
notebook_path = "/workspaces/ETL-TDD/scratchpad.ipynb"

@testbook(notebook_path, execute=['counter'])
def test_first_count(tb):
    """
    Test that the counter can be incremented once.
    """
    count = tb.ref("counter")()
    assert count() == 1
    
    
@testbook(notebook_path, execute=cells_to_test)
def test_arbitrary_count(tb):
    """
    Test that the counter can be incremented an arbitrary number of times.
    """
    count = tb.ref("counter")()
    for i in range(1,29):
        assert count() == i 
    
@testbook(notebook_path, execute=cells_to_test)
def test_initial_positive_count(tb):
    """Test that the counter can be initialised with a positive number."""
    count = tb.ref("counter")(9)
    assert count() == 10
    
@testbook(notebook_path, execute=cells_to_test)
def test_initial_negative_count(tb):
    count = tb.ref("counter")(-10)
    """Test that the counter can be initialised with a negative number."""
    assert count() == -9
        