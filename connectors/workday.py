from __future__ import annotations

from connectors.base import BaseConnector


class Workday(BaseConnector):
    system_name = "Workday"
    system_type = "target"
    date_format = "%Y-%m-%d"
    id_field = "Employee_ID"
    id_prefix = ""
    first_name_field = "First_Name"
    last_name_field = "Last_Name"
    middle_name_field = "Middle_Name"
    salary_field = "Annual_Base_Pay"
    salary_type = "annual"
    status_field = "Worker_Status"
    hire_date_field = "Original_Hire_Date"
    department_field = "Business_Unit"
    location_field = "Work_Location_Name"
    cost_center_field = "Cost_Center_Code"
    manager_id_field = "Manager_Worker_ID"
    position_field = "Position_Title"

    eib_job_org_columns = [
        "Worker_ID", "Effective_Date", "Position", "Business_Title",
        "Organization", "Cost_Center", "Location",
    ]
    eib_hire_date_columns = [
        "Worker_ID", "Original_Hire_Date", "Continuous_Service_Date",
    ]
    eib_status_columns = [
        "Worker_ID", "Effective_Date", "Worker_Status",
        "Termination_Date", "Termination_Reason", "Rehire_Eligible",
    ]
    eib_salary_columns = [
        "Worker_ID", "Effective_Date", "Annual_Base_Pay",
        "Currency", "Pay_Rate_Type", "Frequency",
    ]
