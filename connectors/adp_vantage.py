from __future__ import annotations

from connectors.base import BaseConnector


class ADPVantage(BaseConnector):
    system_name = "ADP Vantage HCM"
    system_type = "legacy"
    date_format = "%Y-%m-%d"
    id_field = "EmployeeID"
    id_prefix = "EMP"
    first_name_field = "FirstName"
    last_name_field = "LastName"
    middle_name_field = "MiddleName"
    salary_field = "AnnualBaseSalary"
    salary_type = "annual"
    status_field = "EmployeeStatus"
    hire_date_field = "HireDate"
    department_field = "Department"
    location_field = "WorkLocation"
    cost_center_field = "CostCenter"
    manager_id_field = "ManagerEmployeeID"
    position_field = "JobTitle"
