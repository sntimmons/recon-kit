import pytest
from src.column_safe import sanitize_column_name

def test_basic():
    assert sanitize_column_name("Employee ID#") == "employee_id"
    assert sanitize_column_name("1st Name") == "col_1st_name"
    assert sanitize_column_name("name; DROP TABLE") == "name_drop_table"
    assert sanitize_column_name("Name!") == "name"
    assert sanitize_column_name("Name@") == "name"
    assert sanitize_column_name("  weird  spaces  ") == "weird_spaces"
    assert sanitize_column_name("UPPER_case") == "upper_case"
    assert sanitize_column_name("123abc") == "col_123abc"
    assert sanitize_column_name("a"*100) == "a"*64

def test_special_chars():
    assert sanitize_column_name("col$name%with^chars") == "col_name_with_chars"
    assert sanitize_column_name("--danger--") == "danger"
    assert sanitize_column_name(";DROP TABLE users;") == "drop_table_users"

def test_empty():
    with pytest.raises(ValueError):
        sanitize_column_name("")
    with pytest.raises(ValueError):
        sanitize_column_name("!!!")
