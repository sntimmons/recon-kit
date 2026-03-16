from __future__ import annotations


class BaseConnector:
    system_name: str = ""
    system_type: str = ""
    date_format: str = ""
    id_field: str = ""
    id_prefix: str = ""
    first_name_field: str = ""
    last_name_field: str = ""
    middle_name_field: str = ""
    salary_field: str = ""
    salary_type: str = ""
    status_field: str = ""
    hire_date_field: str = ""
    department_field: str = ""
    location_field: str = ""
    cost_center_field: str = ""
    manager_id_field: str = ""
    position_field: str = ""

    def to_alias_map(self) -> dict[str, str]:
        # Mapping uses the normalized pipeline field names consumed by src/mapping.py
        alias_map = {
            self.id_field: "worker_id",
            self.first_name_field: "first_name",
            self.last_name_field: "last_name",
            self.middle_name_field: "middle_name",
            self.salary_field: "salary",
            self.status_field: "worker_status",
            self.hire_date_field: "hire_date",
            self.department_field: "district",
            self.location_field: "location",
            self.cost_center_field: "cost_center",
            self.manager_id_field: "manager_id",
            self.position_field: "position",
        }
        return {k: v for k, v in alias_map.items() if k}
