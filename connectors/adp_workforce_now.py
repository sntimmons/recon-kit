from __future__ import annotations

from connectors.base import BaseConnector


class ADPWorkforceNow(BaseConnector):
    system_name = "ADP Workforce Now"
    system_type = "legacy"
    date_format = "%m/%d/%Y"
    id_field = "Associate_ID"
    id_prefix = "EMP"
    first_name_field = "First_Name"
    last_name_field = "Last_Name"
    middle_name_field = "Middle_Initial"
    salary_field = "Annual_Salary"
    salary_type = "annual"
    status_field = "Employment_Status"
    hire_date_field = "Hire_Date"
    department_field = "Department_Name"
    location_field = "Work_Location"
    cost_center_field = "Cost_Center"
    manager_id_field = "Manager_ID"
    position_field = "Job_Title"
